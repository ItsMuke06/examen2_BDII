let chart1
let chart2
let chart3
let chart4
let tabla


// =====================================
// OBTENER FILTROS
// =====================================

function obtenerFiltros() {

    let region = document.getElementById('region').value
    let categoria = document.getElementById('categoria').value
    let subcategoria = document.getElementById('subcategoria').value
    let descuento = document.getElementById('descuento').value

    region = region === "all" ? null : region
    categoria = categoria === "all" ? null : categoria
    subcategoria = subcategoria === "all" ? null : subcategoria
    descuento = descuento === "all" ? null : descuento

    const params = new URLSearchParams()

    if (region) params.append("region", region)
    if (categoria) params.append("categoria", categoria)
    if (subcategoria) params.append("subcategoria", subcategoria)
    if (descuento) params.append("descuento", descuento)

    return params.toString()
}


// =====================================
// CARGAR DASHBOARD
// =====================================

async function cargarDashboard() {

    const query = obtenerFiltros()

    const url = (endpoint) =>
        `/api/${endpoint}${query ? '?' + query : ''}`


    // =====================================
    // KPIS
    // =====================================

    const responseKPIS = await fetch(url('kpis'))
    const kpis = await responseKPIS.json()

    document.getElementById('gananciaTotal').innerText = kpis.ganancia_total
    document.getElementById('perdidaTotal').innerText = kpis.perdida_total
    document.getElementById('productosPerdida').innerText = kpis.productos_perdida
    document.getElementById('descuentoPromedio').innerText = kpis.descuento_promedio


    // =====================================
    // GRAFICO 1
    // =====================================

    const chart1Data = await (await fetch(url('chart1'))).json()

    if (chart1) chart1.destroy()

    chart1 = new Chart(document.getElementById('chart1'), {
        type: 'bar',
        data: {
            labels: chart1Data.labels,
            datasets: [{
                label: 'Ganancia',
                data: chart1Data.values,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true
        }
    })


    // =====================================
    // GRAFICO 2
    // =====================================

    const chart2Data = await (await fetch(url('chart2'))).json()

    if (chart2) chart2.destroy()

    chart2 = new Chart(document.getElementById('chart2'), {
        type: 'pie',
        data: {
            labels: chart2Data.labels,
            datasets: [{
                data: chart2Data.values
            }]
        },
        options: {
            responsive: true
        }
    })


    // =====================================
    // GRAFICO 3
    // =====================================

    const chart3Data = await (await fetch(url('chart3'))).json()

    if (chart3) chart3.destroy()

    chart3 = new Chart(document.getElementById('chart3'), {
        type: 'line',
        data: {
            labels: chart3Data.labels,
            datasets: [{
                label: 'Descuento',
                data: chart3Data.values,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true
        }
    })


    // =====================================
    // GRAFICO 4
    // =====================================

    const chart4Data = await (await fetch(url('chart4'))).json()

    if (chart4) chart4.destroy()

    chart4 = new Chart(document.getElementById('chart4'), {
        type: 'bar',
        data: {
            labels: chart4Data.labels,
            datasets: [{
                label: 'Pérdida',
                data: chart4Data.values,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            indexAxis: 'y'
        }
    })


    // =====================================
    // TABLA
    // =====================================

    const tableData = await (await fetch(url('table'))).json()

    if (tabla) tabla.destroy()

    $('#tablaPerdidas tbody').empty()

    tabla = $('#tablaPerdidas').DataTable({
        data: tableData,
        columns: [
            { data: 'producto' },
            { data: 'categoria' },
            { data: 'subcategoria' },
            { data: 'region' },
            { data: 'ventas' },
            { data: 'descuento' },
            { data: 'ganancia' }
        ]
    })
}


// =====================================
// EVENTOS FILTROS
// =====================================

document.getElementById('region').addEventListener('change', cargarDashboard)
document.getElementById('categoria').addEventListener('change', cargarDashboard)
document.getElementById('subcategoria').addEventListener('change', cargarDashboard)
document.getElementById('descuento').addEventListener('change', cargarDashboard)


// =====================================
// INICIAR DASHBOARD
// =====================================

cargarDashboard()