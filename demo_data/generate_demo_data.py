import string
import random as rd
import pandas as pd

## function to create dummy data
def generate_dummy_data(n=1000):

    rd.seed(368201)

    letters = string.ascii_letters
    letters_up = string.ascii_uppercase

    ## generic id variable
    generic_id = range(1,n+1)

    ## generate table: users
    user_id = generic_id
    username = ("".join(rd.choices(letters, k=rd.randint(4, 20))) for i in range(n))

    data = {"user_id": user_id, "username": username}
    data = pd.DataFrame(data)
    data.to_csv("users.csv", index=False)

    ## generate table: products
    product_id = generic_id
    category = (rd.choice(letters_up) for i in range(n))
    price = (rd.gauss(100, 30) + 50 for i in range(n))
    weight = (rd.expovariate(0.3) for i in range(n))

    data = {"product_id": product_id, "category": category,
            "price": price, "weight": weight}
    data = pd.DataFrame(data)
    data.to_csv("products.csv", index=False)

    ## generate table: reviews
    review_id = generic_id
    fk_user_id = rd.choices(user_id, k=n)
    fk_product_id = rd.choices(product_id, k=n)
    review_text = (" ".join(["text"] * rd.randint(10, 50)) for i in range(n))

    data = {"review_id": review_id, "user_id": fk_user_id,
            "product_id": fk_product_id, "review_text": review_text}
    data = pd.DataFrame(data)
    data.to_json("reviews.json", orient="records", lines=True)

## export 
generate_dummy_data(n=1000)


