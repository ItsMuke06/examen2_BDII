from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


# =========================
# SEGMENTS
# =========================

class Segment(db.Model):
    __tablename__ = 'segments'
    __table_args__ = {'schema': 'superstore'}

    segment_id = db.Column(
        db.Integer,
        primary_key=True
    )

    segment_name = db.Column(
        db.String(100),
        nullable=False,
        unique=True
    )

    customers = db.relationship(
        'Customer',
        back_populates='segment',
        lazy=True
    )


# =========================
# CUSTOMERS
# =========================

class Customer(db.Model):
    __tablename__ = 'customers'
    __table_args__ = {'schema': 'superstore'}

    customer_id = db.Column(
        db.String(50),
        primary_key=True
    )

    customer_name = db.Column(
        db.String(200),
        nullable=False
    )

    segment_id = db.Column(
        db.Integer,
        db.ForeignKey('superstore.segments.segment_id'),
        nullable=False
    )

    segment = db.relationship(
        'Segment',
        back_populates='customers'
    )

    orders = db.relationship(
        'Order',
        back_populates='customer',
        lazy=True
    )


# =========================
# SHIP MODES
# =========================

class ShipMode(db.Model):
    __tablename__ = 'ship_modes'
    __table_args__ = {'schema': 'superstore'}

    ship_mode_id = db.Column(
        db.Integer,
        primary_key=True
    )

    ship_mode_name = db.Column(
        db.String(100),
        nullable=False,
        unique=True
    )

    orders = db.relationship(
        'Order',
        back_populates='ship_mode',
        lazy=True
    )


# =========================
# LOCATIONS
# =========================

class Location(db.Model):
    __tablename__ = 'locations'
    __table_args__ = {'schema': 'superstore'}

    location_id = db.Column(
        db.Integer,
        primary_key=True
    )

    country = db.Column(
        db.String(100),
        nullable=False
    )

    city = db.Column(
        db.String(100),
        nullable=False
    )

    state = db.Column(
        db.String(100),
        nullable=False
    )

    postal_code = db.Column(
        db.String(20)
    )

    region = db.Column(
        db.String(100),
        nullable=False
    )

    orders = db.relationship(
        'Order',
        back_populates='location',
        lazy=True
    )


# =========================
# CATEGORIES
# =========================

class Category(db.Model):
    __tablename__ = 'categories'
    __table_args__ = {'schema': 'superstore'}

    category_id = db.Column(
        db.Integer,
        primary_key=True
    )

    category_name = db.Column(
        db.String(100),
        nullable=False,
        unique=True
    )

    subcategories = db.relationship(
        'SubCategory',
        back_populates='category',
        lazy=True
    )


# =========================
# SUBCATEGORIES
# =========================

class SubCategory(db.Model):
    __tablename__ = 'subcategories'
    __table_args__ = {'schema': 'superstore'}

    subcategory_id = db.Column(
        db.Integer,
        primary_key=True
    )

    subcategory_name = db.Column(
        db.String(100),
        nullable=False
    )

    category_id = db.Column(
        db.Integer,
        db.ForeignKey('superstore.categories.category_id'),
        nullable=False
    )

    category = db.relationship(
        'Category',
        back_populates='subcategories'
    )

    products = db.relationship(
        'Product',
        back_populates='subcategory',
        lazy=True
    )


# =========================
# PRODUCTS
# =========================

class Product(db.Model):
    __tablename__ = 'products'
    __table_args__ = {'schema': 'superstore'}

    product_pk = db.Column(
        db.Integer,
        primary_key=True
    )

    product_code = db.Column(
        db.String(100),
        nullable=False,
        unique=True
    )

    product_name = db.Column(
        db.String(300),
        nullable=False
    )

    subcategory_id = db.Column(
        db.Integer,
        db.ForeignKey('superstore.subcategories.subcategory_id'),
        nullable=False
    )

    subcategory = db.relationship(
        'SubCategory',
        back_populates='products'
    )

    order_details = db.relationship(
        'OrderDetail',
        back_populates='product',
        lazy=True
    )


# =========================
# ORDERS
# =========================

class Order(db.Model):
    __tablename__ = 'orders'
    __table_args__ = {'schema': 'superstore'}

    order_id = db.Column(
        db.String(50),
        primary_key=True
    )

    order_date = db.Column(
        db.Date,
        nullable=False
    )

    ship_date = db.Column(
        db.Date,
        nullable=False
    )

    customer_id = db.Column(
        db.String(50),
        db.ForeignKey('superstore.customers.customer_id'),
        nullable=False
    )

    ship_mode_id = db.Column(
        db.Integer,
        db.ForeignKey('superstore.ship_modes.ship_mode_id'),
        nullable=False
    )

    location_id = db.Column(
        db.Integer,
        db.ForeignKey('superstore.locations.location_id'),
        nullable=False
    )

    customer = db.relationship(
        'Customer',
        back_populates='orders'
    )

    ship_mode = db.relationship(
        'ShipMode',
        back_populates='orders'
    )

    location = db.relationship(
        'Location',
        back_populates='orders'
    )

    order_details = db.relationship(
        'OrderDetail',
        back_populates='order',
        lazy=True
    )


# =========================
# ORDER DETAILS
# =========================

class OrderDetail(db.Model):
    __tablename__ = 'order_details'
    __table_args__ = {'schema': 'superstore'}

    order_detail_id = db.Column(
        db.Integer,
        primary_key=True
    )

    row_id = db.Column(
        db.Integer,
        nullable=False
    )

    order_id = db.Column(
        db.String(50),
        db.ForeignKey('superstore.orders.order_id'),
        nullable=False
    )

    product_pk = db.Column(
        db.Integer,
        db.ForeignKey('superstore.products.product_pk'),
        nullable=False
    )

    sales = db.Column(
        db.Float,
        nullable=False
    )

    quantity = db.Column(
        db.Integer,
        nullable=False
    )

    discount = db.Column(
        db.Float,
        nullable=False
    )

    profit = db.Column(
        db.Float,
        nullable=False
    )

    order = db.relationship(
        'Order',
        back_populates='order_details'
    )

    product = db.relationship(
        'Product',
        back_populates='order_details'
    )