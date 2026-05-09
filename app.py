from flask import (
    Flask,
    render_template,
    jsonify,
    request
)

from config import Config

from models import db

from queries2 import (

    get_kpis,

    ganancias_por_categoria,

    perdidas_por_region,

    descuento_subcategoria,

    productos_mayor_perdida,

    tabla_perdidas
)

# =====================================
# APP
# =====================================

app = Flask(__name__)

app.config.from_object(Config)

db.init_app(app)


# =====================================
# PAGINA PRINCIPAL
# =====================================

@app.route('/')
def index():

    return render_template(
        'index.html'
    )


# =====================================
# OBTENER FILTROS
# =====================================

def obtener_filtros():

    region = request.args.get('region')
    categoria = request.args.get('categoria')
    subcategoria = request.args.get('subcategoria')
    descuento = request.args.get('descuento')

    # =================================
    # LIMPIAR VALORES
    # =================================

    if region == 'all' or region == '':
        region = None

    if categoria == 'all' or categoria == '':
        categoria = None

    if subcategoria == 'all' or subcategoria == '':
        subcategoria = None

    # =================================
    # DESCUENTO
    # =================================

    if (
        descuento == 'all'
        or descuento is None
        or descuento == ''
    ):

        descuento = None

    else:

        try:

            descuento = float(descuento)

        except ValueError:

            descuento = None

    return (

        region,

        categoria,

        subcategoria,

        descuento
    )


# =====================================
# API KPIS
# =====================================

@app.route('/api/kpis')
def api_kpis():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = get_kpis(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# API GRAFICO 1
# GANANCIA POR CATEGORIA
# =====================================

@app.route('/api/chart1')
def api_chart1():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = ganancias_por_categoria(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# API GRAFICO 2
# PERDIDAS POR REGION
# =====================================

@app.route('/api/chart2')
def api_chart2():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = perdidas_por_region(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# API GRAFICO 3
# DESCUENTO SUBCATEGORIA
# =====================================

@app.route('/api/chart3')
def api_chart3():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = descuento_subcategoria(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# API GRAFICO 4
# PRODUCTOS CON MAYOR PERDIDA
# =====================================

@app.route('/api/chart4')
def api_chart4():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = productos_mayor_perdida(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# API TABLA
# =====================================

@app.route('/api/table')
def api_table():

    (
        region,
        categoria,
        subcategoria,
        descuento

    ) = obtener_filtros()

    data = tabla_perdidas(

        region,
        categoria,
        subcategoria,
        descuento
    )

    return jsonify(data)


# =====================================
# MANEJO DE ERRORES
# =====================================

@app.errorhandler(Exception)
def handle_error(error):

    print("ERROR:", error)

    return jsonify({

        'error': str(error)

    }), 500


# =====================================
# EJECUTAR APP
# =====================================

if __name__ == '__main__':

    app.run(
        debug=True
    )