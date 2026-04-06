(function () {
  const POLL_MS = 1500;
  const symbolEl = document.getElementById("symbol");
  const statusEl = document.getElementById("status");
  const booksEl = document.getElementById("books");

  function fmtPrice(p) {
    if (typeof p !== "number" || Number.isNaN(p)) return "—";
    const s = p.toFixed(4).replace(/\.?0+$/, "");
    return s;
  }

  function renderRow(row) {
    const tr = document.createElement("tr");
    tr.innerHTML =
      "<td>" +
      escapeHtml(row.order_id) +
      "</td><td>" +
      fmtPrice(row.price) +
      "</td><td>" +
      escapeHtml(String(row.qty)) +
      "</td>";
    return tr;
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function tableFor(side, rows, sideLabel) {
    const panel = document.createElement("div");
    panel.className = "panel " + (side === "bids" ? "bids" : "asks");
    const h3 = document.createElement("h3");
    h3.textContent = sideLabel;
    panel.appendChild(h3);
    if (!rows || rows.length === 0) {
      const p = document.createElement("p");
      p.className = "empty-book";
      p.textContent = "No levels";
      panel.appendChild(p);
      return panel;
    }
    const tbl = document.createElement("table");
    tbl.innerHTML =
      "<thead><tr><th>Order</th><th>Price</th><th>Qty</th></tr></thead><tbody></tbody>";
    const tb = tbl.querySelector("tbody");
    rows.forEach(function (r) {
      tb.appendChild(renderRow(r));
    });
    panel.appendChild(tbl);
    return panel;
  }

  function renderBooks(data) {
    booksEl.innerHTML = "";
    const symbols = data.symbols || {};
    const names = Object.keys(symbols).sort();
    if (names.length === 0) {
      const p = document.createElement("p");
      p.className = "empty-book";
      p.textContent = "No resting orders in snapshot.";
      booksEl.appendChild(p);
      return;
    }
    names.forEach(function (sym) {
      const book = symbols[sym];
      const block = document.createElement("section");
      block.className = "symbol-block";
      const h2 = document.createElement("h2");
      h2.textContent = sym;
      block.appendChild(h2);
      const grids = document.createElement("div");
      grids.className = "tables";
      grids.appendChild(
        tableFor("bids", book.bids, "Bids")
      );
      grids.appendChild(
        tableFor("asks", book.asks, "Asks")
      );
      block.appendChild(grids);
      booksEl.appendChild(block);
    });
  }

  function setStatus(ok, text) {
    statusEl.textContent = text;
    statusEl.classList.toggle("is-error", !ok);
  }

  async function tick() {
    const sym = symbolEl.value || "ALL";
    try {
      const res = await fetch(
        "/api/snapshot?" + new URLSearchParams({ symbol: sym })
      );
      const data = await res.json();
      if (!res.ok) {
        setStatus(false, "HTTP " + res.status + " — " + (data.error || res.statusText));
        booksEl.innerHTML = "";
        return;
      }
      if (data.error) {
        setStatus(false, data.error);
        booksEl.innerHTML = "";
        return;
      }
      const ts = new Date().toLocaleTimeString();
      const seq =
        data.snapshot_seq != null ? " seq=" + data.snapshot_seq : "";
      setStatus(true, "Updated " + ts + seq);
      renderBooks(data);
    } catch (e) {
      setStatus(false, String(e.message || e));
      booksEl.innerHTML = "";
    }
  }

  symbolEl.addEventListener("change", function () {
    tick();
  });

  tick();
  setInterval(tick, POLL_MS);
})();
