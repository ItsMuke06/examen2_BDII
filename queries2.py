from sqlalchemy import func

from models import (

    db,

    Location,

    Category,

    SubCategory,

    Product,

    Order,

    OrderDetail
)


# =========================================
# FUNCION FILTROS
# =========================================

def aplicar_filtros(

    query,

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    # REGION

    if region and region != 'all':

        query = query.filter(
            Location.region == region
        )

    # CATEGORIA

    if categoria and categoria != 'all':

        query = query.filter(
            Category.category_name == categoria
        )

    # SUBCATEGORIA

    if subcategoria and subcategoria != 'all':

        query = query.filter(
            SubCategory.subcategory_name == subcategoria
        )

    # DESCUENTO

    if descuento is not None:

        query = query.filter(
            OrderDetail.discount >= float(descuento)
        )

    return query

# =========================================
# KPIS
# =========================================

def get_kpis(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(
        OrderDetail
    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = query.join(
        Product,
        OrderDetail.product_pk == Product.product_pk
    )

    query = query.join(
        SubCategory,
        Product.subcategory_id == SubCategory.subcategory_id
    )

    query = query.join(
        Category,
        SubCategory.category_id == Category.category_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    ganancia_total = query.with_entities(

        func.sum(
            OrderDetail.profit
        )

    ).scalar() or 0

    perdida_total = query.filter(

        OrderDetail.profit < 0

    ).with_entities(

        func.sum(
            OrderDetail.profit
        )

    ).scalar() or 0

    productos_perdida = query.filter(

        OrderDetail.profit < 0

    ).count()

    descuento_promedio = query.with_entities(

        func.avg(
            OrderDetail.discount
        )

    ).scalar() or 0

    return {

        'ganancia_total':
            round(
                ganancia_total,
                2
            ),

        'perdida_total':
            round(
                perdida_total,
                2
            ),

        'productos_perdida':
            productos_perdida,

        'descuento_promedio':
            round(
                descuento_promedio,
                2
            )
    }


# =========================================
# GRAFICO 1
# GANANCIA POR CATEGORIA
# =========================================

def ganancias_por_categoria(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(

        Category.category_name,

        func.sum(
            OrderDetail.profit
        )

    )

    query = query.join(
        SubCategory,
        Category.category_id == SubCategory.category_id
    )

    query = query.join(
        Product,
        SubCategory.subcategory_id == Product.subcategory_id
    )

    query = query.join(
        OrderDetail,
        Product.product_pk == OrderDetail.product_pk
    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    query = query.group_by(
        Category.category_name
    )

    results = query.all()

    return {

        'labels':
            [r[0] for r in results],

        'values':
            [float(r[1]) for r in results]
    }


# =========================================
# GRAFICO 2
# PERDIDAS POR REGION
# =========================================

def perdidas_por_region(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(

        Location.region,

        func.sum(
            OrderDetail.profit
        )

    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = query.join(
        Product,
        OrderDetail.product_pk == Product.product_pk
    )

    query = query.join(
        SubCategory,
        Product.subcategory_id == SubCategory.subcategory_id
    )

    query = query.join(
        Category,
        SubCategory.category_id == Category.category_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    query = query.filter(
        OrderDetail.profit < 0
    )

    query = query.group_by(
        Location.region
    )

    results = query.all()

    return {

        'labels':
            [r[0] for r in results],

        'values':
            [float(r[1]) for r in results]
    }


# =========================================
# GRAFICO 3
# DESCUENTO POR SUBCATEGORIA
# =========================================

def descuento_subcategoria(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(

        SubCategory.subcategory_name,

        func.avg(
            OrderDetail.discount
        )

    )

    query = query.join(
        Product,
        SubCategory.subcategory_id == Product.subcategory_id
    )

    query = query.join(
        OrderDetail,
        Product.product_pk == OrderDetail.product_pk
    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = query.join(
        Category,
        SubCategory.category_id == Category.category_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    query = query.group_by(
        SubCategory.subcategory_name
    )

    results = query.all()

    return {

        'labels':
            [r[0] for r in results],

        'values':
            [float(r[1]) for r in results]
    }


# =========================================
# GRAFICO 4
# PRODUCTOS CON MAYOR PERDIDA
# =========================================

def productos_mayor_perdida(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(

        Product.product_name,

        func.sum(
            OrderDetail.profit
        )

    )

    query = query.join(
        OrderDetail,
        Product.product_pk == OrderDetail.product_pk
    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = query.join(
        SubCategory,
        Product.subcategory_id == SubCategory.subcategory_id
    )

    query = query.join(
        Category,
        SubCategory.category_id == Category.category_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    query = query.group_by(
        Product.product_name
    )

    query = query.having(
        func.sum(
            OrderDetail.profit
        ) < 0
    )

    query = query.order_by(
        func.sum(
            OrderDetail.profit
        ).asc()
    )

    query = query.limit(10)

    results = query.all()

    return {

        'labels':
            [r[0] for r in results],

        'values':
            [float(r[1]) for r in results]
    }


# =========================================
# TABLA
# =========================================

def tabla_perdidas(

    region=None,

    categoria=None,

    subcategoria=None,

    descuento=None
):

    query = db.session.query(

        Product.product_name,

        Category.category_name,

        SubCategory.subcategory_name,

        Location.region,

        func.sum(
            OrderDetail.sales
        ),

        func.avg(
            OrderDetail.discount
        ),

        func.sum(
            OrderDetail.profit
        )
    )

    query = query.join(
        OrderDetail,
        Product.product_pk == OrderDetail.product_pk
    )

    query = query.join(
        Order,
        OrderDetail.order_id == Order.order_id
    )

    query = query.join(
        Location,
        Order.location_id == Location.location_id
    )

    query = query.join(
        SubCategory,
        Product.subcategory_id == SubCategory.subcategory_id
    )

    query = query.join(
        Category,
        SubCategory.category_id == Category.category_id
    )

    query = aplicar_filtros(

        query,

        region,

        categoria,

        subcategoria,

        descuento
    )

    query = query.group_by(

        Product.product_name,

        Category.category_name,

        SubCategory.subcategory_name,

        Location.region
    )

    results = query.all()

    data = []

    for row in results:

        data.append({

            'producto':
                row[0],

            'categoria':
                row[1],

            'subcategoria':
                row[2],

            'region':
                row[3],

            'ventas':
                round(
                    float(row[4]),
                    2
                ),

            'descuento':
                round(
                    float(row[5]),
                    2
                ),

            'ganancia':
                round(
                    float(row[6]),
                    2
                )
        })

    return data