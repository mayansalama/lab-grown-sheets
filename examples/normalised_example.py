import random
import faker
import datetime

from labgrownsheets.model import StarSchemaModel

num_iterations = 100000
scale_factor = 4
folder = 'sample-data'

fake = faker.Faker()
low_date = datetime.datetime(2018, 11, 1)
high_date = datetime.datetime(2018, 12, 1)
num_days = (high_date - low_date).days
num_currencies = 5

"""
Usage: python generate_dummy_date.py

Generates a some sample star schema entities for a given number of rows in a specified file. The size of the data 
will be proportional to the number of iterations and the specified scale_factor.
"""


def generate_customer():
    gender = random.sample(["male", "female"], 1)[0]
    if gender == "male":
        name = fake.name_male()
    else:
        name = fake.name_female()
    address = fake.address()

    first, last = name.split(' ')[-2:]
    return {"first_name": first,
            "last_name": last,
            "gender": gender,
            "address": address.replace('\n', ', ')}


def generate_product():
    name = fake.bs().split(' ')[-1]
    desc = fake.paragraph()
    return {
        "name": name,
        "long_desc": desc
    }


def generate_order():
    return {
        'order_time': fake.date_time_between(low_date, high_date)
    }


def generate_order_item():
    return {
        "amount": random.weibullvariate(1, 0.5) * 100
    }


def generate_currency():
    curs = ["AUD"]
    yield {
        "currency": "AUD"
    }
    while True:
        new_cur = fake.currency()[0]
        if new_cur not in curs:
            curs.append(new_cur)
            yield {
                "currency": new_cur
            }


def generate_currency_conv():
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)

    while True:
        root_value = random.weibullvariate(1, 3)

        for cur_day in daterange(low_date, high_date):
            yield {
                "day_value": cur_day,
                "to_aud": root_value
            }
            root_value += random.gauss(0, root_value / 100)


def get_num_products(num_iterations, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_iterations / scale_factor - num_iterations / scale_factor / 2)),
        min(random.randint(150, 200), int(num_iterations / scale_factor + num_iterations / scale_factor / 2))
    )


def main():
    num_products = get_num_products(num_iterations, scale_factor)

    schema = [
        #  DIMS
        ('naive', {
            'name': 'customer',  # the name of the entity/table
            'entity_generator': generate_customer,  # function that defines entity
            'num_iterations': num_iterations  # How many times to run that function
        }),
        ('naive', {
            'name': 'product',
            'entity_generator': generate_product,
            'num_iterations': num_products
        }),
        ('naive', {
            'name': 'currency',
            'entity_generator': generate_currency,
            'num_iterations': num_currencies
        }),
        #  FACTS
        ('naive', {
            'name': 'orders',
            'entity_generator': generate_order,
            'num_iterations': num_iterations * scale_factor,
            'relations': [{'name': 'customer'},
                          {'name': 'currency'}]
        }),
        ('naive', {
            'name': 'order_item',
            'entity_generator': generate_order_item,
            'num_iterations': num_iterations * scale_factor,
            'num_entities_per_iteration': lambda: random.randint(1, 3),  # Number of facts per iteration (e.g. 3 items 1 order)
            'relations': [{'name': 'orders', 'unique': True},
                          {'name': 'product', 'type': 'many_to_many', 'unique': True}]
            # Each iteration has the same entity link for one_to_many relations (e.g. one order_id per order_item)
            # For many_to_many this link is sampled - if unique_per_fact then it is sampled without replacement.
            # In this example an order has multiple order items, each linked to a unique_per_fact product within that order
            # If an order could have multiple of the same product then unique_per_fact would be false
        }),
        ('naive', {
            'name': 'currency_conversion',
            'entity_generator': generate_currency_conv,
            'num_iterations': num_currencies,
            'num_entities_per_iteration': num_days,  # We get one record per currency per day
            'relations': [{'name': 'currency', 'unique': True}]
            # Here the default type is one_to_many - in this case there will be a unique value for each iteration
            # Sampled from the source table - note this will fail if there are more iterations that values in
            # The original table.
        })
    ]

    dummy_data = StarSchemaModel.from_list(schema)
    dummy_data.generate_all_datasets(print_progress=True)
    dummy_data.to_csv(folder)
    dummy_data.to_schemas(folder)
    print("Done")


if __name__ == "__main__":
    main()
