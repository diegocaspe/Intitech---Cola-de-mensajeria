"""Stock real en la tabla products: siembra, descuento atómico en el worker, todo-o-nada, idempotencia, Zona Yape y
«Restablecer stock». SQLite + RabbitMQ falso: no necesitan PostgreSQL."""
import threading
from types import SimpleNamespace

import pytest

from app.database import seed
from app.models.order import OrderStatus
from app.models.yape import EstadoPedidoYape
from app.repositories.sqlite_repository import SqliteOrderRepository, SqliteProductRepository
from app.services.order_service import POISON_SKU, OrderRejected
from tests.test_consumer import deliver
from tests.test_order_service import make, make_product
from tests.test_yape import make as make_yape, pago


def stock(svc, sku):
    return svc.products.get(sku).stock


def items(*lines):
    return [{"sku": sku, "quantity": qty, "unit_price": 10} for sku, qty in lines]


# --- siembra ----------------------------------------------------------------------------------------------------------
def test_la_siembra_solo_llena_la_tabla_vacia_y_no_pisa_el_stock(tmp_path):
    db = str(tmp_path / "tienda.db")
    repo = SqliteOrderRepository(db)
    first = SqliteProductRepository(repo, [make_product("A1", 5), make_product("B2", 3)])
    assert {p.sku: p.stock for p in first.all()} == {"A1": 5, "B2": 3}
    repo._run("UPDATE products SET stock = 1 WHERE sku = 'A1'")       # se vendieron 4
    # otro proceso arranca (api, worker...) con la misma semilla: no duplica filas ni devuelve el stock vendido
    again = SqliteProductRepository(SqliteOrderRepository(db), [make_product("A1", 5), make_product("B2", 3)])
    assert {p.sku: p.stock for p in again.all()} == {"A1": 1, "B2": 3}


class FakeConn:
    """Conexión falsa de psycopg: registra lo que la siembra de PostgreSQL ejecuta."""
    def __init__(self, has_rows):
        self.has_rows, self.inserts = has_rows, []

    def execute(self, sql, args=None):
        return SimpleNamespace(fetchone=lambda: (self.has_rows,))

    def cursor(self):
        conn = self

        class Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def executemany(self, sql, rows):
                conn.inserts.append((sql, list(rows)))
        return Cur()


def test_la_siembra_de_postgres_es_idempotente():
    vacia = FakeConn(has_rows=False)
    n = seed.seed_products(vacia)
    sql, rows = vacia.inserts[0]
    assert n == len(seed.read_catalog()) == len(rows) >= 100
    assert "ON CONFLICT (sku) DO NOTHING" in sql                     # dos arranques a la vez no chocan
    assert len({r[0] for r in rows}) == len(rows)                    # un SKU por fila
    llena = FakeConn(has_rows=True)
    assert seed.seed_products(llena) == 0 and llena.inserts == []    # con datos no toca nada


# --- descuento en el worker --------------------------------------------------------------------------------------------
def test_confirmar_un_pedido_descuenta_su_stock():
    svc, _ = make(seed=[make_product("A1", 5), make_product("B2", 4)])
    order = svc.create_order("a@b.com", items(("A1", 2), ("B2", 1)))
    assert stock(svc, "A1") == 5                                     # crear el pedido no descuenta: lo hace el worker
    svc.process_order(order.id)
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED
    assert (stock(svc, "A1"), stock(svc, "B2")) == (3, 3)


def test_si_no_alcanza_el_stock_el_pedido_se_rechaza_sin_stock_negativo():
    svc, _ = make(seed=[make_product("A1", 1)])
    order = svc.create_order("a@b.com", items(("A1", 2)))
    with pytest.raises(OrderRejected, match="A1"):
        svc.process_order(order.id)
    assert svc.get_order(order.id).status == OrderStatus.REJECTED
    assert stock(svc, "A1") == 1                                     # no se tocó: nunca queda negativo


def test_sin_stock_ocurre_de_forma_natural_al_llegar_a_cero():
    svc, _ = make(seed=[make_product("A1", 2)])
    orders = [svc.create_order(f"c{i}@b.com", items(("A1", 1))) for i in range(3)]
    svc.process_order(orders[0].id)
    svc.process_order(orders[1].id)
    with pytest.raises(OrderRejected):
        svc.process_order(orders[2].id)
    assert [svc.get_order(o.id).status for o in orders] == [OrderStatus.PROCESSED, OrderStatus.PROCESSED,
                                                             OrderStatus.REJECTED]
    assert stock(svc, "A1") == 0


def test_varios_items_es_todo_o_nada():
    svc, _ = make(seed=[make_product("A1", 5), make_product("B2", 0), make_product("C3", 5)])
    order = svc.create_order("a@b.com", items(("C3", 1), ("A1", 2), ("B2", 1)))   # B2 no alcanza
    with pytest.raises(OrderRejected, match="B2"):
        svc.process_order(order.id)
    # A1 se descontó antes que B2 (orden por SKU) y se deshizo: ningún ítem queda descontado
    assert (stock(svc, "A1"), stock(svc, "B2"), stock(svc, "C3")) == (5, 0, 5)
    assert svc.outbox.events("email.compra_confirmada") == []        # tampoco queda un comprobante


def test_un_producto_que_no_existe_no_se_vende():
    svc, _ = make(seed=[make_product("A1", 5)])
    order = svc.create_order("a@b.com", items(("NO-EXISTE", 1)))
    with pytest.raises(OrderRejected):
        svc.process_order(order.id)
    assert svc.get_order(order.id).status == OrderStatus.REJECTED


def test_el_sku_de_prueba_venenoso_no_tiene_stock_que_descontar():
    svc, _ = make(poison=False, seed=[make_product("A1", 5)])
    order = svc.create_order("a@b.com", items((POISON_SKU, 1), ("A1", 1)))
    svc.process_order(order.id)                                      # con el modo apagado se procesa normal
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED and stock(svc, "A1") == 4


# --- idempotencia ------------------------------------------------------------------------------------------------------
def test_un_mensaje_duplicado_no_descuenta_dos_veces():
    svc, _ = make(seed=[make_product("A1", 5)])
    order = svc.create_order("a@b.com", items(("A1", 2)))
    ch1 = deliver(svc, order.id)                                     # RabbitMQ entrega el mismo mensaje dos veces
    ch2 = deliver(svc, order.id)
    assert ch1.acks == ch2.acks == [7]
    assert stock(svc, "A1") == 3                                     # 5 − 2, una sola vez
    svc.process_order(order.id)                                      # y un tercer intento directo tampoco
    assert stock(svc, "A1") == 3


def test_un_pedido_rechazado_por_stock_se_confirma_sin_reintentos_ni_dlq():
    svc, _ = make(seed=[make_product("A1", 0)])
    order = svc.create_order("a@b.com", items(("A1", 1)))
    ch = deliver(svc, order.id)
    assert ch.acks == [7] and ch.nacks == [] and ch.published == []  # resultado de negocio: ack, sin reintento
    assert svc.get_order(order.id).status == OrderStatus.REJECTED


def test_muchos_workers_a_la_vez_nunca_venden_de_mas():
    svc, _ = make(seed=[make_product("A1", 5)])
    orders = [svc.create_order(f"c{i}@b.com", items(("A1", 1))) for i in range(20)]
    threads = [threading.Thread(target=deliver, args=(svc, o.id)) for o in orders]
    [t.start() for t in threads]
    [t.join() for t in threads]
    statuses = [svc.get_order(o.id).status for o in orders]
    assert statuses.count(OrderStatus.PROCESSED) == 5 and statuses.count(OrderStatus.REJECTED) == 15
    assert stock(svc, "A1") == 0


# --- Zona Yape ---------------------------------------------------------------------------------------------------------
def test_yape_descuenta_al_pagar_y_el_worker_no_vuelve_a_descontar():
    svc, orders, _, _ = make_yape(stock=3)
    p = svc.checkout("a@mail.com", "Ana", "YAPE-CASE")
    assert svc.products.get("YAPE-CASE").stock == 3                  # pendiente: reserva, aún no descuenta
    assert svc.stock_disponible()["YAPE-CASE"] == 2
    notif_id, _ = svc.recibir(*pago(p.monto))
    assert svc.procesar(notif_id) == "asociada"
    assert svc.products.get("YAPE-CASE").stock == 2                  # el pago se llevó su unidad
    assert svc.stock_disponible()["YAPE-CASE"] == 2                  # y ya no cuenta como reservado
    orders.process_order(p.id)                                       # el worker confirma sin descontar otra vez
    assert orders.get_order(p.id).status == OrderStatus.PROCESSED
    assert svc.products.get("YAPE-CASE").stock == 2


def test_yape_pago_tardio_sin_stock_se_asocia_pero_no_vende_de_mas():
    svc, orders, _, clock = make_yape(stock=1)
    tarde = svc.checkout("a@mail.com", "A", "YAPE-CASE")
    clock.advance(minutes=9)
    notif_id, _ = svc.recibir(*pago(tarde.monto))                    # yapeó a tiempo...
    clock.advance(minutes=2)
    svc.expirar_vencidos()
    otro = svc.checkout("b@mail.com", "B", "YAPE-CASE")             # ...pero el worker tardó y otro tomó la unidad
    b, _ = svc.recibir(*pago(otro.monto))
    assert svc.procesar(b) == "asociada"
    assert svc.products.get("YAPE-CASE").stock == 0
    assert svc.procesar(notif_id) == "asociada"                      # el pago nunca se pierde
    assert svc.repo.get_pedido(tarde.id).estado == EstadoPedidoYape.PAGADO
    with pytest.raises(OrderRejected):
        orders.process_order(tarde.id)                               # pero no se vende lo que no hay
    assert svc.products.get("YAPE-CASE").stock == 0
    assert svc.estado_cliente(tarde.id, "a@mail.com")["fase"] == "sin_stock"


# --- restablecer stock -------------------------------------------------------------------------------------------------
def test_restablecer_stock_vuelve_a_los_valores_del_catalogo():
    svc, _ = make(seed=[make_product("A1", 5), make_product("B2", 2)])
    order = svc.create_order("a@b.com", items(("A1", 3), ("B2", 2)))
    svc.process_order(order.id)
    assert (stock(svc, "A1"), stock(svc, "B2")) == (2, 0)
    svc.repo._run("DELETE FROM products WHERE sku = 'B2'")           # aunque faltara un producto, vuelve
    assert svc.products.restore_stock() == 2
    assert (stock(svc, "A1"), stock(svc, "B2")) == (5, 2)
    assert svc.products.initial_stock() == {"A1": 5, "B2": 2}
    assert svc.get_order(order.id).status == OrderStatus.PROCESSED   # no toca los pedidos


def test_la_tienda_muestra_el_stock_de_la_tabla():
    svc, _ = make()                                                  # catálogo real sembrado desde catalogo.json
    sku = next(p["sku"] for p in seed.read_catalog() if not p.get("pago_yape_real"))
    inicial = stock(svc, sku)
    order = svc.create_order("a@b.com", items((sku, 1)))
    svc.process_order(order.id)
    assert stock(svc, sku) == inicial - 1
    assert len(svc.products.all()) == len(seed.read_catalog()) + 2   # + A1 y B2 de prueba
