# Anubhav Parbhakar

import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import time
#######################
# FOR TESTING:
# import pandas as pd
#######################


def uninformed(conn: sqlite3.Connection, c: sqlite3.Cursor) -> None:
    # Set scenario to "Uninformed"
    c.execute('PRAGMA automatic_index = 0;')  # autoindex off

    # undefine primary + foreign key
    # undefine for Customers
    c.executescript('''
    ALTER TABLE Customers RENAME TO TEMP;

    CREATE TABLE Customers (
    "customer_id" TEXT,
    "customer_postal_code" INTEGER
    );

    INSERT INTO Customers SELECT
    T.customer_id, T.customer_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # undefine for Sellers
    c.executescript('''
    ALTER TABLE Sellers RENAME TO TEMP;

    CREATE TABLE Sellers (
    "seller_id" TEXT,
    "seller_postal_code" INTEGER
    );

    INSERT INTO Sellers SELECT
    T.seller_id, T.seller_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # undefine for Orders
    c.executescript('''
    ALTER TABLE Orders RENAME TO TEMP;

    CREATE TABLE Orders (
    "order_id" TEXT,
    "customer_id" INTEGER
    );

    INSERT INTO Orders SELECT
    T.order_id, T.customer_id
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # undefine for Order_items
    c.executescript('''
    ALTER TABLE Order_items RENAME TO TEMP;

    CREATE TABLE Order_items (
    "order_id" TEXT,
    "order_item_id" INTEGER,
    "product_id" TEXT,
    "seller_id" TEXT
    );

    INSERT INTO Order_items SELECT
    T.order_id, T.order_item_id, T.product_id, T.seller_id
    FROM TEMP T;

    DROP TABLE TEMP;    
    ''')
    conn.commit()

    return None


def self_optimized(conn: sqlite3.Connection, c: sqlite3.Cursor) -> None:
    # Set scenario to self_optimized
    c.execute('PRAGMA automatic_index = 1;')  # autoindex on

    # redefine primary + foreign key
    # redefine for Customers
    c.executescript('''
    ALTER TABLE Customers RENAME TO TEMP;

    CREATE TABLE Customers (
    "customer_id" TEXT,
    "customer_postal_code" INTEGER,
    PRIMARY KEY ("customer_id")
    );

    INSERT INTO Customers SELECT
    T.customer_id, T.customer_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Sellers
    c.executescript('''
    ALTER TABLE Sellers RENAME TO TEMP;

    CREATE TABLE Sellers (
    "seller_id" TEXT,
    "seller_postal_code" INTEGER,
    PRIMARY KEY ("seller_id")
    );

    INSERT INTO Sellers SELECT
    T.seller_id, T.seller_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Orders
    c.executescript('''
    ALTER TABLE Orders RENAME TO TEMP;

    CREATE TABLE Orders (
    "order_id" TEXT,
    "customer_id" INTEGER,
    PRIMARY KEY ("order_id"),
    FOREIGN KEY ("customer_id") REFERENCES "Customers" ("customer_id")
    );

    INSERT INTO Orders SELECT
    T.order_id, T.customer_id
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Order_items
    c.executescript('''
    ALTER TABLE Order_items RENAME TO TEMP;

    CREATE TABLE Order_items (
    "order_id" TEXT,
    "order_item_id" INTEGER,
    "product_id" TEXT,
    "seller_id" TEXT,
    PRIMARY KEY ("order_id", "order_item_id", "product_id", "seller_id"),
    FOREIGN KEY ("seller_id") REFERENCES "Sellers" ("seller_id"),
    FOREIGN KEY ("order_id") REFERENCES "Orders" ("order_id")
    );

    INSERT INTO Order_items SELECT
    T.order_id, T.order_item_id, T.product_id, T.seller_id
    FROM TEMP T;

    DROP TABLE TEMP;
    ''')

    conn.commit()
    return None


def user_optimized(conn: sqlite3.Connection, c: sqlite3.Cursor) -> None:
    # Set scenario to user_scenario

    # redefine primary + foreign key
    # redefine for Customers
    c.executescript('''
    ALTER TABLE Customers RENAME TO TEMP;

    CREATE TABLE Customers (
    "customer_id" TEXT,
    "customer_postal_code" INTEGER,
    PRIMARY KEY ("customer_id")
    );

    INSERT INTO Customers SELECT
    T.customer_id, T.customer_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Sellers
    c.executescript('''
    ALTER TABLE Sellers RENAME TO TEMP;

    CREATE TABLE Sellers (
    "seller_id" TEXT,
    "seller_postal_code" INTEGER,
    PRIMARY KEY ("seller_id")
    );

    INSERT INTO Sellers SELECT
    T.seller_id, T.seller_postal_code
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Orders
    c.executescript('''
    ALTER TABLE Orders RENAME TO TEMP;

    CREATE TABLE Orders (
    "order_id" TEXT,
    "customer_id" INTEGER,
    PRIMARY KEY ("order_id"),
    FOREIGN KEY ("customer_id") REFERENCES "Customers" ("customer_id")
    );

    INSERT INTO Orders SELECT
    T.order_id, T.customer_id
    FROM TEMP T;
    
    DROP TABLE TEMP;
    ''')

    # redefine for Order_items
    c.executescript('''
    ALTER TABLE Order_items RENAME TO TEMP;

    CREATE TABLE Order_items (
    "order_id" TEXT,
    "order_item_id" INTEGER,
    "product_id" TEXT,
    "seller_id" TEXT,
    PRIMARY KEY ("order_id", "order_item_id", "product_id", "seller_id"),
    FOREIGN KEY ("seller_id") REFERENCES "Sellers" ("seller_id"),
    FOREIGN KEY ("order_id") REFERENCES "Orders" ("order_id")
    );

    INSERT INTO Order_items SELECT
    T.order_id, T.order_item_id, T.product_id, T.seller_id
    FROM TEMP T;

    DROP TABLE TEMP;
    ''')

    # user created indices
    c.execute('CREATE INDEX cid_idx ON Customers(customer_id);')
    c.execute('CREATE INDEX ocid_idx ON Orders(customer_id);')
    c.execute('CREATE INDEX oid_idx ON Orders(order_id);')
    c.execute('CREATE INDEX oiid_idx ON Order_items(order_id);')
    c.execute('CREATE INDEX oisid_idx ON Order_items(seller_id);')
    c.execute('CREATE INDEX ssid_idx ON Sellers(seller_id);')
    c.execute('CREATE INDEX sspc_idx ON Sellers(seller_postal_code);')

    conn.commit()
    return None


def drop_user_indices(conn: sqlite3.Connection, c: sqlite3.Cursor) -> None:
    c.execute('DROP INDEX cid_idx;')
    c.execute('DROP INDEX ocid_idx;')
    c.execute('DROP INDEX oid_idx;')
    c.execute('DROP INDEX oiid_idx;')
    c.execute('DROP INDEX oisid_idx;')
    c.execute('DROP INDEX ssid_idx;')
    c.execute('DROP INDEX sspc_idx;')

    conn.commit()
    return


def query(c: sqlite3.Cursor) -> int:
    # Query 4
    # get random customer id who has atleast one order
    oIcustomerID = c.execute(f'''SELECT customer_id
    FROM Orders
    GROUP BY customer_id
    HAVING COUNT(DISTINCT order_id) > 1
    ORDER BY RANDOM() 
    LIMIT 1;''').fetchone()

    # return no. of unique postal codes for the orders, for a rand customer who has atleast one order
    if oIcustomerID != None:
        return c.execute(f'''SELECT COUNT (DISTINCT s.seller_postal_code)
        FROM Customers c, Orders o, Order_items oi, Sellers s
        WHERE c.customer_id = {oIcustomerID[0]} AND o.order_id = oi.order_id AND oi.seller_id = s.seller_id;
        ''').fetchone()[0]
    else:
        return -1

# INNER JOIN Orders o ON c.customer_id = {oIcustomerID[0]}
# INNER JOIN Order_items oi ON o.order_id = oi.order_id
# INNER JOIN Sellers s ON oi.seller_id = s.seller_id


def connect(path: str):
    conn = sqlite3.Connection(path)
    c = conn.cursor()

    return conn, c


def disconnect(conn: sqlite3.Connection) -> None:
    conn.close()
    return None


def main() -> None:
    # tuple containing scenario names (for chart)
    scenarios = {
        "Uninformed": np.zeros(3),
        "Self-Optimized": np.zeros(3),
        "User-Optimized": np.zeros(3)
    }
    keys = tuple(key for key in scenarios.keys())
    # tuple containing database names (for chart)
    dbs = (
        "SmallDB",
        "MediumDB",
        "LargeDB"
    )

    databases = ("A3Small.db", "A3Medium.db", "A3Large.db")
    scenario_functions = {
        "Uninformed": uninformed,
        "Self-Optimized": self_optimized,
        "User-Optimized": user_optimized
    }

    for i in range(0, len(dbs)):
        path = databases[i]
        for scenario in keys:
            conn, c = connect(path)

            scenario_functions[scenario](conn, c)
            start = time.time()
            for _ in range(0, 50):
                query(c)
            end = time.time()
            scenarios[scenario][i] = (end - start)/50  # avg time
            if scenario == 'User-Optimized':
                drop_user_indices(conn, c)

            disconnect(conn)

    # CREATE GRAPH
    width = 0.5
    fig, ax = plt.subplots()
    bottom = np.zeros(3)
    for optimization, count in scenarios.items():
        p = ax.bar(dbs, count, width, label=optimization, bottom=bottom)
        bottom += count

    ax.set_title("Query (runtime in ms)")
    # ax.legend(loc = "upper center", ncol = 3)
    ax.legend(loc="upper center")

    plt.savefig("queryChart.png")

    # for testing
    # a = pd.DataFrame.from_dict(scenarios)
    # a.index = dbs
    # display(a)

    return


main()
