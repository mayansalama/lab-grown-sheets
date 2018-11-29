import random
import faker
import datetime

from labgrownsheets.model import StarSchemaModel

num_iterations = 1000
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


def generate_order(*args, **kwargs):
    #  If args are supported then arg[0] will be the full datasets dictionary
    #  Kwargs will have 'inst': {dict of relations and ids}
    if not kwargs:  # Note that functions are tested to see if they compile, so the first run through will be null
        ld = low_date
        hd = high_date
    else:  # This way we never get an order created before a customer is
        ld = args[0]['customer'][kwargs['customer_id']][0]['valid_from_timestamp']
        hd = high_date
    return {
        'order_time': fake.date_time_between(ld, hd)
    }


def get_num_products(num_iterations, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_iterations / scale_factor - num_iterations / scale_factor / 2)),
        min(random.randint(150, 200), int(num_iterations / scale_factor + num_iterations / scale_factor / 2))
    )


def main():
    num_products = get_num_products(num_iterations, scale_factor)

    schema = [
        #  DIMS
        ('naive_type2_scd', {
            'name': 'customer',
            'entity_generator': generate_customer,
            'num_iterations': num_iterations,
            'mutation_rate': 0.95,  # Will update mutate cols 30% of the time
            'mutating_cols': ['address']  # Only address will update
        }),
        ('naive', {
            'name': 'product',
            'entity_generator': generate_product,
            'num_iterations': num_products
        }),
        #  FACTS
        ('naive', {
            'name': 'orders',
            'entity_generator': generate_order,
            'num_iterations': num_iterations * scale_factor,
            'relations': [{'name': 'customer'}, {'name': 'product'}]
        })]

    dummy_data = StarSchemaModel.from_list(schema)
    dummy_data.generate_all_datasets(print_progress=True)
    dummy_data.to_csv(folder)
    dummy_data.to_schemas(folder)
    print("Done")


if __name__ == "__main__":
    main()
