ddl_query = '''

create table sub_category(
    sub_category_key int not null primary key,
    category varchar(255),
    sub_category varchar(255)
);

create table product(
    product_key int not null primary key,
    sub_category_key int references sub_category(sub_category_key),
    product_id varchar(255), 
    product_name varchar(255)
);
    
create table state(
    state_key int not null primary key, 
    state varchar(255),
    region varchar(255),
    country varchar(255)
);

create table postal_code(
    location_key int not null primary key,
    state_key int references state(state_key), 
    postal_code int,
    city varchar(255)
);

create table segment(
    segment_key int not null primary key,
    segment varchar (255)
);

create table customer(
    customer_key int not null primary key,
    segment_key int references segment(segment_key),
    customer_id varchar(255),
    customer_name varchar(255)
);

create table ship_mode(
    ship_mode_key int not null primary key,
    ship_mode varchar(255)
);

create table total_orders(
    order_id varchar(255),
    order_date timestamp,
    location_key int references postal_code(location_key),
    customer_key int references customer(customer_key),
    product_key int references product(product_key),
    ship_mode_key int references ship_mode(ship_mode_key),
    profit decimal(9,2),
    quantity int,
    sales decimal(9,2),
    discount decimal(9,2)
);
'''