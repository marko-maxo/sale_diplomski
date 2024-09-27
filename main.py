import json
from typing import List

from sqlalchemy import select, delete
import random

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


def populate_data():
    print("Dropping existing data")

    db_session.execute(delete(Transaction))
    db_session.execute(delete(AccountBalance))
    db_session.execute(delete(Account))
    db_session.execute(delete(Customer))
    db_session.execute(delete(Branch))
    db_session.execute(delete(Customer))

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
    input('1) Populate prestage\n2) Populate stage\n3) Populate DWH\n4)Populate DWH first time\n555) Populate data\nPick: ')
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
