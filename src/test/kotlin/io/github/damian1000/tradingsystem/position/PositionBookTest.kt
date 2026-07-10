package io.github.damian1000.tradingsystem.position

import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.tradingsystem.consume.Fill
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PositionBookTest {
    private fun fill(
        symbol: String = "SIM",
        side: Side = Side.BID,
        size: Long = 5,
        price: String = "101.00",
        ts: Long = 1000,
    ) = Fill(symbol, BigDecimal(price), size, 1, 2, side, ts)

    @Test
    fun `buys accumulate a long position`() {
        val book = PositionBook()
        book.apply(fill(size = 5))
        val position = book.apply(fill(size = 3, price = "102.00", ts = 2000))
        assertEquals(Position("SIM", 8, BigDecimal("102.00"), 2000), position)
    }

    @Test
    fun `sells net down and can go short`() {
        val book = PositionBook()
        book.apply(fill(size = 5))
        val position = book.apply(fill(side = Side.OFFER, size = 8, ts = 2000))
        assertEquals(-3, position.quantity)
    }

    @Test
    fun `a flattened symbol stays on the book at zero`() {
        val book = PositionBook()
        book.apply(fill(size = 5))
        val position = book.apply(fill(side = Side.OFFER, size = 5, ts = 2000))
        assertEquals(0, position.quantity)
        assertEquals(1, book.all().size, "a symbol that traded stays visible when flat")
    }

    @Test
    fun `positions are tracked per symbol and listed in symbol order`() {
        val book = PositionBook()
        book.apply(fill(symbol = "ZZZ"))
        book.apply(fill(symbol = "AAA", side = Side.OFFER, size = 2))
        assertEquals(listOf("AAA", "ZZZ"), book.all().map { it.symbol })
        assertEquals(-2, book.positionOf("AAA")?.quantity)
        assertEquals(5, book.positionOf("ZZZ")?.quantity)
        assertNull(book.positionOf("MISSING"))
    }

    @Test
    fun `restore replaces in-memory state with the persisted book`() {
        val book = PositionBook()
        book.apply(fill(symbol = "OLD"))
        val persisted = Position("SIM", 7, BigDecimal("99.50"), 500)
        book.restore(listOf(persisted))
        assertEquals(listOf(persisted), book.all())
        assertNull(book.positionOf("OLD"))
        assertEquals(12, book.apply(fill(size = 5)).quantity, "new fills build on the restored quantity")
    }
}
