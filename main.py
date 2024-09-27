import json
from typing import List
from sqlalchemy import inspect
from datetime import datetime
from sqlalchemy import select, delete
import random

from models import (
    Branch, PreStageBranch, StageBranch, DWHBranch,
    Account, PreStageAccount, StageAccount, DWHAccount,
    AccountBalance, PreStageAccountBalance, StageAccountBalance, DWHAccountBalance,
    Transaction, PreStageTransaction, StageTransaction, DWHTransaction,
    Customer, PreStageCustomer, StageCustomer, DWHCustomer,
    Currency, PreStageCurrency, StageCurrency, DWHCurrency
)
from database import Base, db_session
import hashlib

default_date = datetime(9999, 7, 1)


def drop_prestage_branch():
    db_session.execute(delete(PreStageBranch))


def drop_prestage_account():
    db_session.execute(delete(PreStageAccount))


def drop_prestage_transaction():
    db_session.execute(delete(PreStageTransaction))


def drop_prestage_customer():
    db_session.execute(delete(PreStageCustomer))


def drop_prestage_account_balance():
    db_session.execute(delete(PreStageAccountBalance))

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
            'branch_id': str(branch.id)
        }

        branch_hash = hashlib.md5(
            json.dumps(branch_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_branch = PreStageBranch()
        new_prestage_branch.name = branch_data['name']
        new_prestage_branch.branch_id = branch_data['branch_id']
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
            'account_id': str(account.id),
            'account_type': str(account.account_type),
            'date_created': str(account.date_created),
            'status': str(account.status),
            'customer_id': str(account.customer_id),  # int?
        }

        account_hash = hashlib.md5(
            json.dumps(account_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_account = PreStageAccount()
        new_prestage_account.account_id = account_data['account_id']
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
            'currency_id': str(currency.id),
            'code': str(currency.code),
            'name': str(currency.name),
            'exchange_to_base_currency': str(currency.exchange_to_base_currency),
        }

        currency_hash = hashlib.md5(
            json.dumps(currency_data, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()

        new_prestage_currency = PreStageCurrency()
        new_prestage_currency.currency_id = currency_data['currency_id']
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
            'customer_id': str(customer.id),
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

        new_prestage_customer = PreStageCustomer()
        new_prestage_customer.customer_id = customer_data['customer_id']
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
        new_prestage_account_balance.account_balance_id = str(account_balance.id)
        new_prestage_account_balance.balance = str(account_balance.balance)
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
        new_prestage_transaction.transaction_id = str(transaction.id)
        db_session.add(new_prestage_transaction)

    db_session.commit()

    print(f'Commited prestage transaction changes. Added {len(transactions_data)} prestage transaction rows.')

###

def drop_stage_branch():
    db_session.execute(delete(StageBranch))


def drop_stage_account():
    db_session.execute(delete(StageAccount))


def drop_stage_transaction():
    db_session.execute(delete(StageTransaction))


def drop_stage_account_balance():
    db_session.execute(delete(StageAccountBalance))


def drop_stage_currency():
    db_session.execute(delete(StageCurrency))


def drop_stage_customer():
    db_session.execute(delete(StageCustomer))


def populate_stage_branch():

    drop_stage_branch()

    prestage_branch_data = db_session.scalars(
        select(
            PreStageBranch
        ).order_by(
            PreStageBranch.id
        )
    ).all()

    cnt = 0

    for prestage_branch in prestage_branch_data:

        dwh_branch = db_session.scalars(
            select(
                DWHBranch
            ).where(
                DWHBranch.branch_id == prestage_branch.branch_id,
                DWHBranch.bus_date_until == default_date
            )
        ).first()

        if dwh_branch is None or (dwh_branch and dwh_branch.checksum != prestage_branch.checksum):
            new_stage_branch = StageBranch()
            new_stage_branch.branch_id = prestage_branch.branch_id
            new_stage_branch.name = prestage_branch.name
            new_stage_branch.country = prestage_branch.country
            new_stage_branch.city = prestage_branch.city
            new_stage_branch.date_created = prestage_branch.date_created
            new_stage_branch.date_closed = prestage_branch.date_closed
            new_stage_branch.checksum = prestage_branch.checksum
            new_stage_branch.unid = prestage_branch.unid
            new_stage_branch.bus_date_from = prestage_branch.bus_date_from
            db_session.add(new_stage_branch)
            cnt += 1

    db_session.commit()

    print(f'Commited stage branch changes. Added {cnt} stage branch rows.')


def populate_stage_account():

    drop_stage_account()

    prestage_account_data = db_session.scalars(
        select(
            PreStageAccount
        ).order_by(
            PreStageAccount.id
        )
    ).all()

    cnt = 0

    for prestage_account in prestage_account_data:

        dwh_account = db_session.scalars(
            select(
                DWHAccount
            ).where(
                DWHAccount.account_id == prestage_account.account_id,
                DWHAccount.bus_date_until == default_date
            )
        ).first()

        if dwh_account is None or (dwh_account and dwh_account.checksum != prestage_account.checksum):
            new_stage_account = StageAccount()
            new_stage_account.account_id = prestage_account.account_id
            new_stage_account.account_type = prestage_account.account_type
            new_stage_account.date_created = prestage_account.date_created
            new_stage_account.status = prestage_account.status
            new_stage_account.customer_id = prestage_account.customer_id
            new_stage_account.checksum = prestage_account.checksum
            new_stage_account.unid = prestage_account.unid
            new_stage_account.bus_date_from = prestage_account.bus_date_from
            db_session.add(new_stage_account)
            cnt += 1

    db_session.commit()

    print(f'Commited stage account changes. Added {cnt} stage account rows.')


def populate_stage_currency():

    drop_stage_currency()

    prestage_currency_data = db_session.scalars(
        select(
            PreStageCurrency
        ).order_by(
            PreStageCurrency.id
        )
    ).all()

    cnt = 0

    for prestage_currency in prestage_currency_data:

        dwh_currency = db_session.scalars(
            select(
                DWHCurrency
            ).where(
                DWHCurrency.currency_id == prestage_currency.currency_id,
                DWHCurrency.bus_date_until == default_date
            )
        ).first()

        if dwh_currency is None or (dwh_currency and dwh_currency.checksum != prestage_currency.checksum):
            new_stage_currency = StageCurrency()
            new_stage_currency.currency_id = prestage_currency.currency_id
            new_stage_currency.code = prestage_currency.code
            new_stage_currency.name = prestage_currency.name
            new_stage_currency.exchange_to_base_currency = prestage_currency.exchange_to_base_currency
            db_session.add(new_stage_currency)
            cnt += 1

    db_session.commit()

    print(f'Commited stage currency changes. Added {cnt} stage currency rows.')


def populate_stage_customer():

    drop_stage_customer()

    prestage_customer_data = db_session.scalars(
        select(
            PreStageCustomer
        ).order_by(
            PreStageCustomer.id
        )
    ).all()

    cnt = 0

    for prestage_customer in prestage_customer_data:

        dwh_customer = db_session.scalars(
            select(
                DWHCustomer
            ).where(
                DWHCustomer.customer_id == prestage_customer.customer_id,
                DWHCustomer.bus_date_until == default_date
            )
        ).first()

        if dwh_customer is None or (dwh_customer and dwh_customer.checksum != prestage_customer.checksum):
            new_stage_customer = StageCustomer()
            new_stage_customer.customer_id = prestage_customer.customer_id
            new_stage_customer.first_name = prestage_customer.first_name
            new_stage_customer.last_name = prestage_customer.last_name
            new_stage_customer.gender = prestage_customer.gender
            new_stage_customer.city = prestage_customer.city
            new_stage_customer.country = prestage_customer.country
            new_stage_customer.type = prestage_customer.type
            new_stage_customer.date_created = prestage_customer.date_created
            new_stage_customer.date_closed = prestage_customer.date_closed
            db_session.add(new_stage_customer)
            cnt += 1

    db_session.commit()

    print(f'Commited stage customer changes. Added {cnt} stage customer rows.')


def populate_stage_account_balance():

    drop_prestage_account_balance()

    prestage_account_balance_data = db_session.scalars(
        select(
            PreStageAccountBalance
        ).order_by(
            PreStageAccountBalance.id
        )
    ).all()

    for prestage_account_balance in prestage_account_balance_data:
        new_stage_account_balance = StageAccountBalance()
        new_stage_account_balance.account_balance_id = prestage_account_balance.account_balance_id
        new_stage_account_balance.account_id = prestage_account_balance.account_id
        new_stage_account_balance.currency_id = prestage_account_balance.currency_id
        new_stage_account_balance.balance = prestage_account_balance.balance
        new_stage_account_balance.account_balance_date = prestage_account_balance.account_balance_date
        db_session.add(new_stage_account_balance)

    db_session.commit()

    print(f'Commited stage account_balance changes. Added {len(prestage_account_balance_data)} stage account_balance rows.')


def populate_stage_transaction():

    drop_stage_transaction()

    prestage_transaction_data = db_session.scalars(
        select(
            PreStageTransaction
        ).order_by(
            PreStageTransaction.id
        )
    ).all()

    for prestage_transaction in prestage_transaction_data:
        new_stage_transaction = StageTransaction()
        new_stage_transaction.transaction_id = prestage_transaction.transaction_id
        new_stage_transaction.account_id = prestage_transaction.branch_id
        new_stage_transaction.branch_id = prestage_transaction.branch_id
        new_stage_transaction.currency_id = prestage_transaction.currency_id
        new_stage_transaction.amount = prestage_transaction.amount
        new_stage_transaction.success = prestage_transaction.success
        new_stage_transaction.date = prestage_transaction.date
        db_session.add(new_stage_transaction)

    db_session.commit()

    print(f'Commited stage transaction changes. Added {len(prestage_transaction_data)} stage transaction rows.')


def populate_prestage():
    populate_prestage_branch()
    populate_prestage_account()
    populate_prestage_currency()
    populate_prestage_customer()
    populate_prestage_account_balance()
    populate_prestage_transaction()


def populate_stage():
    populate_stage_branch()
    populate_stage_account()
    populate_stage_currency()
    populate_stage_customer()
    populate_stage_account_balance()
    populate_stage_transaction()


def populate_dwh():
    pass


def populate_dwh_first_time():
    print("REMOVING OLD DWH")
    db_session.execute(delete(DWHBranch))
    db_session.execute(delete(DWHAccount))
    db_session.execute(delete(DWHCurrency))
    db_session.execute(delete(DWHCustomer))
    db_session.execute(delete(DWHTransaction))
    db_session.execute(delete(DWHAccountBalance))

    print("ADDING INITIAL DWH")
    ### BRANCHES
    prestage_branches = db_session.scalars(select(PreStageBranch)).all()
    for pre_stage_branch in prestage_branches:
        dwh_branch = DWHBranch()
        for i in inspect(pre_stage_branch).mapper.column_attrs:
            if i.key == 'id':
                continue
            elif i.key == 'date_closed':
                if getattr(pre_stage_branch, i.key) in [None, 'None']:
                    setattr(dwh_branch, 'status', 'active')
                else:
                    setattr(dwh_branch, 'status', 'closed')
            setattr(dwh_branch, i.key, str(getattr(pre_stage_branch, i.key)))
        db_session.add(dwh_branch)
    db_session.commit()
    print("ADDED BRANCHES")
    ###
    ### CURRENCIES
    prestage_currencies = db_session.scalars(select(PreStageCurrency)).all()
    for pre_stage_currency in prestage_currencies:
        dwh_currency = DWHCurrency()
        for i in inspect(pre_stage_currency).mapper.column_attrs:
            if i.key == 'id':
                continue
            elif i.key == 'exchange_to_base_currency':
                if getattr(pre_stage_currency, i.key) in [None, 'False']:
                    setattr(dwh_currency, i.key, False)
                else:
                    setattr(dwh_currency, i.key, True)
            else:
                setattr(dwh_currency, i.key, str(getattr(pre_stage_currency, i.key)))
        db_session.add(dwh_currency)
    db_session.commit()
    print("ADDED CURRENCIES")
    ###
    ### CUSTOMERS
    prestage_customers = db_session.scalars(select(PreStageCustomer)).all()
    for pre_stage_customer in prestage_customers:
        dwh_customer = DWHCustomer()
        for i in inspect(pre_stage_customer).mapper.column_attrs:
            if i.key == 'id':
                continue
            elif i.key == 'date_closed':
                if getattr(pre_stage_customer, i.key) in [None, 'None']:
                    setattr(dwh_customer, 'status', 'active')
                else:
                    setattr(dwh_customer, 'status', 'closed')
                setattr(dwh_customer, i.key, str(getattr(pre_stage_customer, i.key)))
            elif i.key in ['first_name', 'last_name']:
                setattr(dwh_customer, 'customer_name', str(
                    getattr(pre_stage_customer, i.key) + " " +
                    str(getattr(dwh_customer, 'customer_name') if getattr(dwh_customer, 'customer_name') else "")
                ))
            else:
                setattr(dwh_customer, i.key, str(getattr(pre_stage_customer, i.key)))
        db_session.add(dwh_customer)
    db_session.commit()
    print("ADDED CUSTOMERS")
    ###
    ### ACCOUNTS
    prestage_accounts = db_session.scalars(select(PreStageAccount)).all()
    for pre_stage_account in prestage_accounts:
        dwh_account = DWHAccount()
        for i in inspect(pre_stage_account).mapper.column_attrs:
            if i.key == 'id':
                continue
            else:
                setattr(dwh_account, i.key, str(getattr(pre_stage_account, i.key)))
        db_session.add(dwh_account)
    db_session.commit()
    print("ADDED ACCOUNTS")
    ###
    ### ACCOUNT BALANCES
    prestage_account_balances = db_session.scalars(select(PreStageAccountBalance)).all()
    for pre_stage_account_balance in prestage_account_balances:
        dwh_account_balance = DWHAccountBalance()
        for i in inspect(pre_stage_account_balance).mapper.column_attrs:
            if i.key == 'id':
                continue
            if i.key == 'balance':
                if float(getattr(pre_stage_account_balance, i.key)) < 0:
                    setattr(dwh_account_balance, 'status', ' in debt')
                else:
                    setattr(dwh_account_balance, 'status', ' ok')
                setattr(dwh_account_balance, i.key, str(getattr(pre_stage_account_balance, i.key)))
            else:
                setattr(dwh_account_balance, i.key, str(getattr(pre_stage_account_balance, i.key)))
        setattr(dwh_account_balance, 'quality_identificator', 0)
        db_session.add(dwh_account_balance)
    db_session.commit()
    print("ADDED ACCOUNT BALANCES")
    ###
    ### TRANSACTION
    prestage_transactions = db_session.scalars(select(PreStageTransaction)).all()
    for pre_stage_transaction in prestage_transactions:
        dwh_transaction = DWHTransaction()
        for i in inspect(pre_stage_transaction).mapper.column_attrs:
            if i.key == 'id':
                continue
            elif i.key == 'amount':
                if float(getattr(pre_stage_transaction, i.key)) > 0:
                    setattr(dwh_transaction, 'type', 'inflow')
                else:
                    setattr(dwh_transaction, 'type', 'outflow')
                setattr(dwh_transaction, i.key, str(getattr(pre_stage_transaction, i.key)))
            elif i.key == 'success':
                if getattr(pre_stage_transaction, i.key) in [None, 'False']:
                    setattr(dwh_transaction, i.key, False)
                else:
                    setattr(dwh_transaction, i.key, True)
            else:
                setattr(dwh_transaction, i.key, str(getattr(pre_stage_transaction, i.key)))
        setattr(dwh_transaction, 'quality_identificator', 0)
        db_session.add(dwh_transaction)
    db_session.commit()
    print("ADDED TRANSACTIONS")

def populate_data():
    print("Dropping existing data")

    db_session.execute(delete(Transaction))
    db_session.execute(delete(AccountBalance))
    db_session.execute(delete(Account))
    db_session.execute(delete(Customer))
    db_session.execute(delete(Branch))
    db_session.execute(delete(Currency))

    print("Adding new data")
    customers = [
        {
            "first_name": "Nikola",
            "last_name": "Nikolic",
            "gender": "M",
            "city": "Belgrade",
            "country": "Serbia",
            "type": "fizicko_lice",
            "date_created": "2021-02-20",
        },
        {
            "first_name": "Ana",
            "last_name": "Anic",
            "gender": "F",
            "city": "Nis",
            "country": "Serbia",
            "type": "fizicko_lice",
            "date_created": "2022-02-20",
        },
        {
            "first_name": "Luka",
            "last_name": "Lukic",
            "gender": "M",
            "city": "Madrid",
            "country": "Spain",
            "type": "pravno_lice",
            "date_created": "2023-12-20",
        },
        {
            "first_name": "Bojana",
            "last_name": "Bojanic",
            "gender": "F",
            "city": "Paris",
            "country": "France",
            "type": "pravno_lice",
            "date_created": "2019-03-15",
        },
        {
            "first_name": "Filip",
            "last_name": "Filipovic",
            "gender": "M",
            "city": "Cacak",
            "country": "Serbia",
            "type": "fizicko_lice",
            "date_created": "2022-11-13",
        },
    ]
    currencies = [
        {
            "code": "USD",
            "name": "Dollar",
            "exchange_to_base_currency": True
        },
        {
            "code": "RSD",
            "name": "Dinar",
            "exchange_to_base_currency": False
        },
        {
            "code": "EUR",
            "name": "Euro",
            "exchange_to_base_currency": True
        },
    ]
    branches = [
        {
            "name": "Filijala Beograd",
            "country": "Serbia",
            "city": "Belgrade",
            "date_created": "2018-01-01",
        },
        {
            "name": "Filijala Nis",
            "country": "Serbia",
            "city": "Nis",
            "date_created": "2020-03-03",
            "date_closed": "2023-03-01",
        },
        {
            "name": "Filijala Madrid",
            "country": "Spain",
            "city": "Madrid",
            "date_created": "2018-01-01",
        },
    ]
    transactions = [
        {
            "account_id": None,
            "branch_id": None,
            "currency_id": None,
            "amount": 100,
            "success": True,
            "date": "2018-01-01",
        },
        {
            "account_id": None,
            "branch_id": None,
            "currency_id": None,
            "amount": 12,
            "success": True,
            "date": "2019-01-01",
        },
        {
            "account_id": None,
            "branch_id": None,
            "currency_id": None,
            "amount": 230,
            "success": True,
            "date": "2024-09-27",
        },
        {
            "account_id": None,
            "branch_id": None,
            "currency_id": None,
            "amount": 5000,
            "success": False,
            "date": "2022-11-11",
        },
    ]
    accounts = [
        {
            "customer_id": None,
            "account_type": "stedni",
            "date_created": "2021-10-11",
            "status": "aktivan"
        },
        {
            "customer_id": None,
            "account_type": "tekuci",
            "date_created": "2021-10-11",
            "status": "aktivan"
        },
        {
            "customer_id": None,
            "account_type": "tekuci",
            "date_created": "2021-10-11",
            "date_closed": "2023-10-11",
            "status": "zatvoren"
        },
        {
            "customer_id": None,
            "account_type": "stedni",
            "date_created": "2022-10-13",
            "status": "aktivan"
        },
    ]
    account_balances = [
        {
            "account_id": None,
            "currency_id": None,
            "balance": 200,
            "account_balance_date": "2021-10-11",
        },
        {
            "account_id": None,
            "currency_id": None,
            "balance": 345,
            "account_balance_date": "2022-03-11",
        },
        {
            "account_id": None,
            "currency_id": None,
            "balance": 12,
            "account_balance_date": "2022-11-11",
        },
        {
            "account_id": None,
            "currency_id": None,
            "balance": 500,
            "account_balance_date": "2024-09-27",
        },
    ]

    ### Currencies
    currencies_db = []
    for c in currencies:
        new_currency = Currency()
        new_currency.code = c.get("code")
        new_currency.name = c.get("name")
        new_currency.exchange_to_base_currency = c.get("exchange_to_base_currency")

        db_session.add(new_currency)
        db_session.flush()
        currencies_db.append(new_currency)
    db_session.commit()
    print("Added currencies")

    ### Customers
    customers_db = []
    for c in customers:
        new_customer = Customer()
        new_customer.first_name = c.get("first_name")
        new_customer.last_name = c.get("last_name")
        new_customer.gender = c.get("gender")
        new_customer.city = c.get("city")
        new_customer.country = c.get("country")
        new_customer.type = c.get("type")
        new_customer.date_created = c.get("date_created")
        new_customer.date_closed = c.get("date_closed")

        db_session.add(new_customer)
        db_session.flush()
        customers_db.append(new_customer)
    db_session.commit()
    print("Added customers")

    ### Branches
    branches_db = []
    for b in branches:
        new_branch = Branch()
        new_branch.name = b.get("name")
        new_branch.city = b.get("city")
        new_branch.country = b.get("country")
        new_branch.date_created = b.get("date_created")
        new_branch.date_closed = b.get("date_closed")

        db_session.add(new_branch)
        db_session.flush()
        branches_db.append(new_branch)
    db_session.commit()
    print("Added branches")

    ### Accounts
    accounts_db = []
    for a in accounts:
        new_account = Account()
        new_account.customer_id = random.choice(customers_db).id
        new_account.account_type = a.get("account_type")
        new_account.status = a.get("status")
        new_account.date_created = a.get("date_created")

        db_session.add(new_account)
        db_session.flush()
        accounts_db.append(new_account)
    db_session.commit()
    print("Added accounts")

    ### Account balance
    account_balances_db = []
    for a in account_balances:
        new_account_balance = AccountBalance()
        new_account_balance.account_id = random.choice(accounts_db).id
        new_account_balance.currency_id = random.choice(currencies_db).id
        new_account_balance.balance = a.get("balance")
        new_account_balance.account_balance_date = a.get("account_balance_date")

        db_session.add(new_account_balance)
        db_session.flush()
        account_balances_db.append(new_account_balance)
    db_session.commit()
    print("Added account balances")

    ### Transactions
    transactions_db = []
    for t in transactions:
        new_transaction = Transaction()
        new_transaction.account_id = random.choice(accounts_db).id
        new_transaction.currency_id = random.choice(currencies_db).id
        new_transaction.branch_id = random.choice(branches_db).id
        new_transaction.amount = t.get("amount")
        new_transaction.date = t.get("date")
        new_transaction.success = t.get("success")

        db_session.add(new_transaction)
        db_session.flush()
        transactions_db.append(new_transaction)
    db_session.commit()
    print("Added transactions")

    ### DONE

    print("Data has been added")


pick = int(
    input(
        '1) Populate prestage\n2) Populate stage\n3) Populate DWH\n4) Populate DWH first time\n555) Populate data\nPick: ')
)

match pick:
    case 1:
        populate_prestage()
    case 2:
        populate_stage()
    case 3:
        populate_dwh()
    case 4:
        populate_dwh_first_time()
    case 555:
        populate_data()
