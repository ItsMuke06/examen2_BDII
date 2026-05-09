from flask import Flask
from config import Config

from sqlalchemy import func

from models import (
    db,
    Customer,
    Location,
    Category,
    SubCategory,
    Product,
    Order,
    OrderDetail,
    ShipMode,
    Segment
)

app = Flask(__name__)

app.config.from_object(Config)

db.init_app(app)


with app.app_context():

# ==================================================
    # CONSULTA 1
    # ==================================================
    # Total de ventas, ganancias acumuladas,
    # promedio de descuento y cantidad total vendida
    # agrupado por región y categoría
    # ordenado por mayor rentabilidad
    # ==================================================

    consulta_1 = (

        db.session.query(

            Location.region.label('region'),

            Category.category_name.label('category'),

            func.sum(OrderDetail.sales).label('total_sales'),

            func.sum(OrderDetail.profit).label('total_profit'),

            func.avg(OrderDetail.discount).label('avg_discount'),

            func.sum(OrderDetail.quantity).label('total_quantity')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .group_by(
            Location.region,
            Category.category_name
        )

        .order_by(
            func.sum(OrderDetail.profit).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 1 ================\n')

    for row in consulta_1:

        print(
            f'Region: {row.region} | '
            f'Categoria: {row.category} | '
            f'Ventas: {row.total_sales} | '
            f'Ganancia: {row.total_profit} | '
            f'Descuento Promedio: {row.avg_discount} | '
            f'Cantidad Vendida: {row.total_quantity}'
        )


    # ==================================================
    # CONSULTA 2
    # ==================================================
    # TOP 15 clientes más rentables
    # ==================================================

    consulta_2 = (

        db.session.query(

            Customer.customer_name.label('customer'),

            func.sum(OrderDetail.sales).label('total_sales'),

            func.sum(OrderDetail.profit).label('total_profit'),

            func.count(
                func.distinct(Order.order_id)
            ).label('total_orders'),

            func.avg(OrderDetail.sales).label('avg_ticket'),

            func.avg(OrderDetail.discount).label('avg_discount')

        )

        .join(
            Order,
            Customer.customer_id == Order.customer_id
        )

        .join(
            OrderDetail,
            Order.order_id == OrderDetail.order_id
        )

        .group_by(
            Customer.customer_name
        )

        .order_by(
            func.sum(OrderDetail.profit).desc()
        )

        .limit(15)

        .all()
    )

    print('\n================ CONSULTA 2 ================\n')

    for row in consulta_2:

        print(
            f'Cliente: {row.customer} | '
            f'Ventas Totales: {row.total_sales} | '
            f'Ganancia Total: {row.total_profit} | '
            f'Pedidos: {row.total_orders} | '
            f'Ticket Promedio: {row.avg_ticket} | '
            f'Descuento Promedio: {row.avg_discount}'
        )

    # ==================================================
    # CONSULTA 3
    # ==================================================
    # Determinar las subcategorías que generan
    # pérdidas acumuladas por región
    # ==================================================

    consulta_3 = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            SubCategory.subcategory_name.label(
                'subcategory'
            ),

            func.sum(
                OrderDetail.profit
            ).label('total_loss'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('affected_orders'),

            func.avg(
                OrderDetail.discount
            ).label('avg_discount')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .group_by(
            Location.region,
            SubCategory.subcategory_name
        )

        .having(
            func.sum(
                OrderDetail.profit
            ) < 0
        )

        .order_by(
            func.sum(
                OrderDetail.profit
            ).asc()
        )

        .all()
    )

    print('\n================ CONSULTA 3 ================\n')

    for row in consulta_3:

        print(
            f'Region: {row.region} | '
            f'Subcategoria: {row.subcategory} | '
            f'Perdida Total: {row.total_loss} | '
            f'Pedidos Afectados: {row.affected_orders} | '
            f'Descuento Promedio: {row.avg_discount}'
        )


    # ==================================================
    # CONSULTA 4
    # ==================================================
    # Margen de rentabilidad por producto
    # Margen = Profit / Sales
    # Mostrar solo productos:
    # - con más de 20 ventas
    # - con ganancias positivas
    # ==================================================

    consulta_4 = (

        db.session.query(

            Product.product_name.label(
                'product'
            ),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            (
                func.sum(
                    OrderDetail.profit
                ) /

                func.sum(
                    OrderDetail.sales
                )
            ).label('profit_margin'),

            func.count(
                OrderDetail.order_detail_id
            ).label('total_sales_count')

        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .group_by(
            Product.product_name
        )

        .having(
            func.count(
                OrderDetail.order_detail_id
            ) > 20
        )

        .having(
            func.sum(
                OrderDetail.profit
            ) > 0
        )

        .order_by(
            (
                func.sum(
                    OrderDetail.profit
                ) /

                func.sum(
                    OrderDetail.sales
                )
            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 4 ================\n')

    for row in consulta_4:

        print(
            f'Producto: {row.product} | '
            f'Ventas Totales: {row.total_sales} | '
            f'Ganancia Total: {row.total_profit} | '
            f'Margen: {row.profit_margin} | '
            f'Cantidad Ventas: {row.total_sales_count}'
        )

    # ==================================================
    # CONSULTA 5
    # ==================================================
    # Clientes que compraron productos
    # pertenecientes a más de 4 categorías
    # ==================================================

    consulta_5 = (

        db.session.query(

            Customer.customer_name.label(
                'customer'
            ),

            func.count(
                func.distinct(
                    Category.category_id
                )
            ).label('total_categories'),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders')

        )

        .join(
            Order,
            Customer.customer_id == Order.customer_id
        )

        .join(
            OrderDetail,
            Order.order_id == OrderDetail.order_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .group_by(
            Customer.customer_name
        )

        .having(
            func.count(
                func.distinct(
                    Category.category_id
                )
            ) > 2
        )

        .order_by(
            func.sum(
                OrderDetail.sales
            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 5 ================\n')

    for row in consulta_5:

        print(
            f'Cliente: {row.customer} | '
            f'Categorias: {row.total_categories} | '
            f'Ventas: {row.total_sales} | '
            f'Ganancias: {row.total_profit} | '
            f'Pedidos: {row.total_orders}'
        )
# ==================================================
    # CONSULTA 7
    # ==================================================
    # Productos vendidos en la mayor cantidad
    # de ciudades diferentes
    # ==================================================

    consulta_7 = (

        db.session.query(

            Product.product_name.label(
                'product'
            ),

            func.count(
                func.distinct(
                    Location.city
                )
            ).label('total_cities'),

            func.count(
                func.distinct(
                    Location.region
                )
            ).label('total_regions'),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit')

        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .group_by(
            Product.product_name
        )

        .order_by(
            func.count(
                func.distinct(
                    Location.city
                )
            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 7 ================\n')

    for row in consulta_7:

        print(
            f'Producto: {row.product} | '
            f'Ciudades: {row.total_cities} | '
            f'Regiones: {row.total_regions} | '
            f'Ventas: {row.total_sales} | '
            f'Ganancias: {row.total_profit}'
        )
    # ==================================================
    # CONSULTA 8
    # ==================================================
    # Regiones donde:
    # - descuento promedio > 25%
    # - ganancias negativas
    # - pedidos superiores al promedio
    # ==================================================

    subquery_orders = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders')

        )

        .select_from(Order)

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .group_by(
            Location.region
        )

        .subquery()
    )

    subquery_promedio_pedidos = (

        db.session.query(

            func.avg(
                subquery_orders.c.total_orders
            )

        ).scalar_subquery()
    )

    consulta_8 = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            func.avg(
                OrderDetail.discount
            ).label('avg_discount'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders')

        )

        .select_from(OrderDetail)

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .group_by(
            Location.region
        )

        .having(
            func.avg(
                OrderDetail.discount
            ) > 0.25
        )

        .having(
            func.sum(
                OrderDetail.profit
            ) < 0
        )

        .having(

            func.count(
                func.distinct(
                    Order.order_id
                )
            )

            >

            subquery_promedio_pedidos

        )

        .order_by(
            func.sum(
                OrderDetail.profit
            ).asc()
        )

        .all()
    )

    print('\n================ CONSULTA 8 ================\n')

    for row in consulta_8:

        print(
            f'Region: {row.region} | '
            f'Descuento Promedio: {row.avg_discount} | '
            f'Ganancia Total: {row.total_profit} | '
            f'Pedidos: {row.total_orders}'
        )
    # ==================================================
    # CONSULTA 9
    # ==================================================
    # Ticket promedio por segmento y región
    # ==================================================

    consulta_9 = (

        db.session.query(

            Segment.segment_name.label(
                'segment'
            ),

            Location.region.label(
                'region'
            ),

            func.avg(
                OrderDetail.sales
            ).label('avg_ticket'),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders')

        )

        .join(
            Customer,
            Segment.segment_id == Customer.segment_id
        )

        .join(
            Order,
            Customer.customer_id == Order.customer_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            OrderDetail,
            Order.order_id == OrderDetail.order_id
        )

        .group_by(
            Segment.segment_name,
            Location.region
        )

        .order_by(
            func.avg(
                OrderDetail.sales
            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 9 ================\n')

    for row in consulta_9:

        print(
            f'Segmento: {row.segment} | '
            f'Region: {row.region} | '
            f'Ticket Promedio: {row.avg_ticket} | '
            f'Ventas: {row.total_sales} | '
            f'Ganancias: {row.total_profit} | '
            f'Pedidos: {row.total_orders}'
        )

# ==================================================
    # CONSULTA 10
    # ==================================================
    # Pedidos con mayores pérdidas económicas
    # ==================================================

    consulta_10 = (

        db.session.query(

            Customer.customer_name.label(
                'customer'
            ),

            Location.region.label(
                'region'
            ),

            Category.category_name.label(
                'category'
            ),

            Product.product_name.label(
                'product'
            ),

            func.sum(
                OrderDetail.profit
            ).label('total_loss'),

            func.avg(
                OrderDetail.discount
            ).label('avg_discount')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Customer,
            Order.customer_id == Customer.customer_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .group_by(
            Customer.customer_name,
            Location.region,
            Category.category_name,
            Product.product_name
        )

        .having(
            func.sum(
                OrderDetail.profit
            ) < 0
        )

        .order_by(
            func.sum(
                OrderDetail.profit
            ).asc()
        )

        .all()
    )

    print('\n================ CONSULTA 10 ================\n')

    for row in consulta_10:

        print(
            f'Cliente: {row.customer} | '
            f'Region: {row.region} | '
            f'Categoria: {row.category} | '
            f'Producto: {row.product} | '
            f'Perdida Total: {row.total_loss} | '
            f'Descuento: {row.avg_discount}'
        )


    # ==================================================
    # CONSULTA 11
    # ==================================================
    # Clientes con comportamiento multicategoría
    # avanzado
    # ==================================================

    consulta_11 = (

        db.session.query(

            Customer.customer_name.label(
                'customer'
            ),

            func.count(
                func.distinct(
                    Location.region
                )
            ).label('total_regions'),

            func.count(
                func.distinct(
                    SubCategory.subcategory_id
                )
            ).label('total_subcategories'),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders')

        )

        .join(
            Order,
            Customer.customer_id == Order.customer_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            OrderDetail,
            Order.order_id == OrderDetail.order_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .group_by(
            Customer.customer_name
        )

        .having(
            func.count(
                func.distinct(
                    SubCategory.subcategory_id
                )
            ) > 5
        )

        .having(
            func.count(
                func.distinct(
                    Location.region
                )
            ) > 2
        )

        .having(
            func.count(
                func.distinct(
                    Order.order_id
                )
            ) > 10
        )

        .order_by(
            func.sum(
                OrderDetail.sales
            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 11 ================\n')

    for row in consulta_11:

        print(
            f'Cliente: {row.customer} | '
            f'Regiones: {row.total_regions} | '
            f'Subcategorias: {row.total_subcategories} | '
            f'Ventas: {row.total_sales} | '
            f'Pedidos: {row.total_orders}'
        )
    # ==================================================
    # CONSULTA 12
    # ==================================================
    # Categoría líder por región
    # (la categoría con mayores ventas)
    # ==================================================

    subquery_ventas_categoria = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            Category.category_name.label(
                'category'
            ),

            func.sum(
                OrderDetail.sales
            ).label('total_sales')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .group_by(
            Location.region,
            Category.category_name
        )

        .subquery()
    )

    subquery_max_region = (

        db.session.query(

            subquery_ventas_categoria.c.region,

            func.max(
                subquery_ventas_categoria.c.total_sales
            ).label('max_sales')

        )

        .group_by(
            subquery_ventas_categoria.c.region
        )

        .subquery()
    )

    consulta_12 = (

        db.session.query(

            subquery_ventas_categoria.c.region,

            subquery_ventas_categoria.c.category,

            subquery_ventas_categoria.c.total_sales

        )

        .join(

            subquery_max_region,

            (
                subquery_ventas_categoria.c.region
                ==
                subquery_max_region.c.region
            )

        )

        .filter(

            subquery_ventas_categoria.c.total_sales
            ==
            subquery_max_region.c.max_sales

        )

        .all()
    )

    print('\n================ CONSULTA 12 ================\n')

    for row in consulta_12:

        print(
            f'Region: {row.region} | '
            f'Categoria Lider: {row.category} | '
            f'Ventas Totales: {row.total_sales}'
        )


    # ==================================================
    # CONSULTA 13
    # ==================================================
    # Participación porcentual de ventas
    # por categoría respecto al total regional
    # ==================================================

    subquery_total_region = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            func.sum(
                OrderDetail.sales
            ).label('regional_sales')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .group_by(
            Location.region
        )

        .subquery()
    )

    consulta_13 = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            Category.category_name.label(
                'category'
            ),

            func.sum(
                OrderDetail.sales
            ).label('category_sales'),

            (
                (
                    func.sum(
                        OrderDetail.sales
                    )

                    /

                    subquery_total_region.c.regional_sales

                ) * 100

            ).label('participation_percentage')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .join(

            subquery_total_region,

            Location.region
            ==
            subquery_total_region.c.region

        )

        .group_by(
            Location.region,
            Category.category_name,
            subquery_total_region.c.regional_sales
        )

        .order_by(
            Location.region
        )

        .all()
    )

    print('\n================ CONSULTA 13 ================\n')

    for row in consulta_13:

        print(
            f'Region: {row.region} | '
            f'Categoria: {row.category} | '
            f'Ventas Categoria: {row.category_sales} | '
            f'Participacion: {row.participation_percentage}%'
        )


    # ==================================================
    # CONSULTA 14
    # ==================================================
    # Detectar anomalías comerciales:
    # productos con descuentos altos
    # pero ganancias negativas
    # ==================================================

    consulta_14 = (

        db.session.query(

            Product.product_name.label(
                'product'
            ),

            Category.category_name.label(
                'category'
            ),

            func.avg(
                OrderDetail.discount
            ).label('avg_discount'),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            func.count(
                OrderDetail.order_detail_id
            ).label('transactions')

        )

        .join(
            Product,
            OrderDetail.product_pk == Product.product_pk
        )

        .join(
            SubCategory,
            Product.subcategory_id == SubCategory.subcategory_id
        )

        .join(
            Category,
            SubCategory.category_id == Category.category_id
        )

        .group_by(
            Product.product_name,
            Category.category_name
        )

        .having(
            func.avg(
                OrderDetail.discount
            ) > 0.30
        )

        .having(
            func.sum(
                OrderDetail.profit
            ) < 0
        )

        .order_by(
            func.sum(
                OrderDetail.profit
            ).asc()
        )

        .all()
    )

    print('\n================ CONSULTA 14 ================\n')

    for row in consulta_14:

        print(
            f'Producto: {row.product} | '
            f'Categoria: {row.category} | '
            f'Descuento Promedio: {row.avg_discount} | '
            f'Ventas: {row.total_sales} | '
            f'Ganancia: {row.total_profit} | '
            f'Transacciones: {row.transactions}'
        )
    # ==================================================
    # CONSULTA 15
    # ==================================================
    # Ranking de regiones según rentabilidad
    # y eficiencia comercial
    # ==================================================

    consulta_15 = (

        db.session.query(

            Location.region.label(
                'region'
            ),

            func.sum(
                OrderDetail.sales
            ).label('total_sales'),

            func.sum(
                OrderDetail.profit
            ).label('total_profit'),

            func.avg(
                OrderDetail.discount
            ).label('avg_discount'),

            func.count(
                func.distinct(
                    Order.order_id
                )
            ).label('total_orders'),

            func.count(
                func.distinct(
                    Customer.customer_id
                )
            ).label('total_customers'),

            (
                func.sum(
                    OrderDetail.profit
                )

                /

                func.sum(
                    OrderDetail.sales
                )

            ).label('profit_margin')

        )

        .join(
            Order,
            OrderDetail.order_id == Order.order_id
        )

        .join(
            Customer,
            Order.customer_id == Customer.customer_id
        )

        .join(
            Location,
            Order.location_id == Location.location_id
        )

        .group_by(
            Location.region
        )

        .having(
            func.sum(
                OrderDetail.sales
            ) > 50000
        )

        .order_by(
            (
                func.sum(
                    OrderDetail.profit
                )

                /

                func.sum(
                    OrderDetail.sales
                )

            ).desc()
        )

        .all()
    )

    print('\n================ CONSULTA 15 ================\n')

    for row in consulta_15:

        print(
            f'Region: {row.region} | '
            f'Ventas Totales: {row.total_sales} | '
            f'Ganancia Total: {row.total_profit} | '
            f'Descuento Promedio: {row.avg_discount} | '
            f'Pedidos: {row.total_orders} | '
            f'Clientes: {row.total_customers} | '
            f'Margen Rentabilidad: {row.profit_margin}'
        )
