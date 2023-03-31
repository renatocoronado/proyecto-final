dw_query = '''
create table product_dimension(
    product_id varchar(255) primary key, 
    category varchar(255),
    sub_category varchar(255),
    product_name varchar(255)
);

create table date_dimension(
    date_key int not null primary key,
    full_date timestamp, 
    day_of_week int,
    day_num_in_month int,
    day_num_overall int,
    day_name varchar(255),
    day_abbrev varchar(255),
    weekday_flag varchar(255),
    week_num_in_year int,
    week_num_overall int,
    week_begin_date date,
    week_begin_date_key int,
    month_ int,
    month_num_overall int,
    month_name varchar(255),
    month_abbrev varchar(255),
    quarter_ int,
    year_ int,
    yearmo int, 
    fiscal_month int,
    fiscal_quarter int,
    fiscal_year int,
    last_day_in_month_flag varchar(255),
    same_day_year_ago_date date
);

create table location_dimension(
    postal_code int not null primary key, 
    city varchar(255),
    state varchar(255),
    region varchar(255),
    country varchar(255)
);

create table customer_dimension(
    customer_id varchar(255) primary key,
    customer_name varchar(255),
    segment varchar (255)
);

create table order_dimension(
    order_id varchar(255) primary key,
    order_date timestamp,
    ship_date timestamp,
    ship_mode varchar(255)
);

create table retail_sales_fact(
    date_key int references date_dimension(date_key),
    product_id varchar(255) references product_dimension(product_id),
    postal_code int references location_dimension(postal_code),
    customer_id varchar(255) references customer_dimension(customer_id),
    order_id varchar(255) references order_dimension(order_id),
    profit decimal(9,2),
    quantity int,
    sales decimal(9,2),
    discount decimal(9,2)
);
'''