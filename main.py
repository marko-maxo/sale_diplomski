import json

from sqlalchemy import select

from models import (
    Branch, PreStageBranch,
    Account, PreStageAccount,
    AccountBalance, PreStageAccountBalance,
    Transaction, PreStageTransaction,
    Customer, PreStageCustomer,
    Currency, PreStageCurrency,
)
from database import Base, db_session
import hashlib


def populate_prestage_branch():
    branch_data = db_session.scalars(
        select(
            Branch
        ).order_by(
            Branch.id
        )
    ).all()
    branch_hashes_and_data = {}
    branch_hashes = []
    for branch in branch_data:
        branch_data = {
            'name': str(branch.name),
            'city': str(branch.city),
            'country': str(branch.country),
            'date_created': str(branch.date_created),
            'date_closed': str(branch.date_closed),
        }

        branch_hash = hashlib.md5(json.dumps(branch_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()
        branch_data['branch_data_hash'] = branch_hash

        branch_hashes_and_data[branch_hash] = branch_data
        branch_hashes.append(branch_hash)

    prestage_hashes = db_session.scalars(
        select(
            PreStageBranch.checksum
        ).where(
            PreStageBranch.checksum.in_(branch_hashes)
        )
    ).all()

    if prestage_hashes:
        data_hashes_to_add = [
            data_branch_hash for data_branch_hash in branch_hashes if data_branch_hash not in prestage_hashes
        ]
    else:
        data_hashes_to_add = branch_hashes_and_data

    for data_to_add_hash in data_hashes_to_add:
        new_prestage_branch = PreStageBranch()
        new_prestage_branch.name = branch_hashes_and_data[data_to_add_hash]['name']
        new_prestage_branch.city = branch_hashes_and_data[data_to_add_hash]['city']
        new_prestage_branch.country = branch_hashes_and_data[data_to_add_hash]['country']
        new_prestage_branch.date_created = branch_hashes_and_data[data_to_add_hash]['date_created']
        new_prestage_branch.date_closed = branch_hashes_and_data[data_to_add_hash]['date_closed']
        new_prestage_branch.checksum = branch_hashes_and_data[data_to_add_hash]['branch_data_hash']
        db_session.add(new_prestage_branch)

    db_session.commit()

    print(f'Commited prestage branch changes. Added {len(data_hashes_to_add)} prestage branches rows.')

def populate_prestage():
    populate_prestage_branch()

populate_prestage()