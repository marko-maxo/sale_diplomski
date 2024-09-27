import json
from typing import List

from sqlalchemy import select, delete

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

from models.branch import DWHBranch


def drop_prestage_branch():
    db_session.execute(delete(PreStageBranch))


def drop_prestage_account():
    db_session.execute(delete(PreStageAccount))


def drop_prestage_transaction():
    db_session.execute(delete(PreStageTransaction))


def drop_prestage_customer():
    db_session.execute(delete(PreStageCustomer))


def drop_prestage_account_balance():
    db_session.execute(delete(AccountBalance))


def drop_prestage_currency():
    db_session.execute(delete(PreStageCurrency))


def populate_prestage_branch():
    drop_prestage_branch()

    branches_data = db_session.scalars(
        select(
            Branch
        ).order_by(
            Branch.id
        )
    ).all()

    for branch in branches_data:
        branch_data = {
            'name': str(branch.name),
            'city': str(branch.city),
            'country': str(branch.country),
            'date_created': str(branch.date_created),
            'date_closed': str(branch.date_closed),
        }

        branch_hash = hashlib.md5(
            json.dumps(branch_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_branch = PreStageBranch()
        new_prestage_branch.name = branch_data['name']
        new_prestage_branch.city = branch_data['city']
        new_prestage_branch.country = branch_data['country']
        new_prestage_branch.date_created = branch_data['date_created']
        new_prestage_branch.date_closed = branch_data['date_closed']
        new_prestage_branch.checksum = branch_hash
        db_session.add(new_prestage_branch)

    db_session.commit()

    print(f'Commited prestage branch changes. Added {len(branches_data)} prestage branches rows.')


def populate_prestage_account():
    drop_prestage_account()

    accounts_data = db_session.scalars(
        select(
            Account
        ).order_by(
            Account.id
        )
    ).all()

    for account in accounts_data:
        account_data = {
            'account_type': str(account.account_type),
            'date_created': str(account.date_created),
            'status': str(account.status),
            'customer_id': str(account.customer_id),  # int?
        }

        account_hash = hashlib.md5(
            json.dumps(account_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_account = PreStageAccount()
        new_prestage_account.account_type = account_data['account_type']
        new_prestage_account.date_created = account_data['date_created']
        new_prestage_account.status = account_data['status']
        new_prestage_account.customer_id = account_data['customer_id']
        new_prestage_account.checksum = account_hash
        db_session.add(new_prestage_account)

    db_session.commit()

    print(f'Commited prestage account changes. Added {len(accounts_data)} prestage accounts rows.')


def populate_prestage_currency():
    drop_prestage_currency()

    currencies_data = db_session.scalars(
        select(
            Currency
        ).order_by(
            Currency.id
        )
    ).all()

    for currency in currencies_data:
        currency_data = {
            'code': str(currency.code),
            'name': str(currency.name),
            'exchange_to_base_currency': str(currency.exchange_to_base_currency),
        }

        currency_hash = hashlib.md5(
            json.dumps(currency_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_currency = PreStageCurrency()
        new_prestage_currency.code = currency_data['code']
        new_prestage_currency.name = currency_data['name']
        new_prestage_currency.exchange_to_base_currency = currency_data['exchange_to_base_currency']
        new_prestage_currency.checksum = currency_hash
        db_session.add(new_prestage_currency)

    db_session.commit()

    print(f'Commited prestage currency changes. Added {len(currencies_data)} prestage currency rows.')


def populate_prestage_customer():
    drop_prestage_customer()

    customers_data = db_session.scalars(
        select(
            Customer
        ).order_by(
            Customer.id
        )
    ).all()

    for customer in customers_data:
        customer_data = {
            'first_name': str(customer.first_name),
            'last_name': str(customer.last_name),
            'gender': str(customer.gender),
            'city': str(customer.city),
            'country': str(customer.country),
            'type': str(customer.type),
            'date_created': str(customer.date_created),
            'date_closed': str(customer.date_closed)
        }

        customer_hash = hashlib.md5(
            json.dumps(customer_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_customer = PreStageCurrency()
        new_prestage_customer.first_name = customer_data['first_name']
        new_prestage_customer.last_name = customer_data['last_name']
        new_prestage_customer.gender = customer_data['gender']
        new_prestage_customer.city = customer_data['city']
        new_prestage_customer.country = customer_data['country']
        new_prestage_customer.type = customer_data['type']
        new_prestage_customer.date_created = customer_data['date_created']
        new_prestage_customer.date_closed = customer_data['date_closed']
        new_prestage_customer.checksum = customer_hash
        db_session.add(new_prestage_customer)

    db_session.commit()

    print(f'Commited prestage customer changes. Added {len(customers_data)} prestage customer rows.')


def populate_prestage_account_balance():
    drop_prestage_account_balance()

    accounts_balance_data = db_session.scalars(
        select(
            AccountBalance
        ).order_by(
            AccountBalance.id
        )
    ).all()

    for account_balance in accounts_balance_data:
        new_prestage_account_balance = PreStageAccountBalance()
        new_prestage_account_balance.account_id = str(account_balance.account_id)
        new_prestage_account_balance.currency_id = str(account_balance.currency_id)
        new_prestage_account_balance.account_balance_date = str(account_balance.account_balance_date)
        db_session.add(new_prestage_account_balance)

    db_session.commit()

    print(
        f'Commited prestage account_balance changes. Added {len(accounts_balance_data)} prestage account_balance rows.')


def populate_prestage_transaction():
    drop_prestage_transaction()

    transactions_data = db_session.scalars(
        select(
            Transaction
        ).order_by(
            Transaction.id
        )
    ).all()

    for transaction in transactions_data:
        new_prestage_transaction = PreStageTransaction()
        new_prestage_transaction.account_id = str(transaction.account_id)
        new_prestage_transaction.branch_id = str(transaction.branch_id)
        new_prestage_transaction.currency_id = str(transaction.currency_id)
        new_prestage_transaction.amount = str(transaction.amount)
        new_prestage_transaction.success = str(transaction.success)
        new_prestage_transaction.date = str(transaction.date)
        db_session.add(new_prestage_transaction)

    db_session.commit()

    print(f'Commited prestage transaction changes. Added {len(transactions_data)} prestage transaction rows.')


###

def populate_prestage():
    populate_prestage_branch()
    populate_prestage_account()
    populate_prestage_currency()
    populate_prestage_customer()
    populate_prestage_account_balance()
    populate_prestage_transaction()


def populate_stage():
    pass


def populate_dwh():
    pass


def populate_dwh_first_time():
    prestage_branch: List[PreStageBranch] = db_session.scalars(select(PreStageBranch)).all()
    for branch in prestage_branch:
        dwh_branch = DWHBranch()



pick = int(input('1) Populate prestage\n2) Populate stage\n3) Populate DWH\n4)Populate DWH first time'))

match pick:
    case 1:
        populate_prestage()
    case 2:
        populate_stage()
    case 3:
        populate_dwh()
    case 4:
        populate_dwh_first_time()
