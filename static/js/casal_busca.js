(function () {
  function qs(root, selector) {
    return root.querySelector(selector);
  }

  function qsa(root, selector) {
    return Array.from(root.querySelectorAll(selector));
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function criarPopover() {
    let popover = document.getElementById("casal-busca-popover");

    if (popover) {
      return popover;
    }

    popover = document.createElement("div");
    popover.id = "casal-busca-popover";
    popover.className = "casal-busca-popover";
    popover.innerHTML = `
      <div class="casal-busca-card">
        <div class="casal-busca-header">
          <strong>Selecione o casal</strong>
          <button type="button" class="casal-busca-close" aria-label="Fechar">&times;</button>
        </div>
        <div class="casal-busca-body"></div>
      </div>
    `;

    document.body.appendChild(popover);

    qs(popover, ".casal-busca-close").addEventListener("click", function () {
      fecharPopover();
    });

    popover.addEventListener("click", function (event) {
      if (event.target === popover) {
        fecharPopover();
      }
    });

    return popover;
  }

  function fecharPopover() {
    const popover = document.getElementById("casal-busca-popover");
    if (popover) {
      popover.classList.remove("is-open");
      qs(popover, ".casal-busca-body").innerHTML = "";
    }
  }

  function preencherCampos(grupo, casal) {
    const inputId = qs(grupo, "[data-casal-id]");
    const inputEle = qs(grupo, "[data-casal-ele]");
    const inputEla = qs(grupo, "[data-casal-ela]");
    const inputTelefone = qs(grupo, "[data-casal-telefones]");
    const inputEndereco = qs(grupo, "[data-casal-endereco]");

    if (inputId) inputId.value = casal.id || "";
    if (inputEle) inputEle.value = casal.nome_usual_ele || "";
    if (inputEla) inputEla.value = casal.nome_usual_ela || "";
    if (inputTelefone) inputTelefone.value = casal.telefones || "";
    if (inputEndereco) inputEndereco.value = casal.endereco || "";

    grupo.dispatchEvent(new CustomEvent("casalSelecionado", {
      bubbles: true,
      detail: { casal: casal }
    }));
  }

  function montarItem(casal) {
    const apelidosEle = casal.apelidos_ele ? `<div><small>Apelidos ele: ${escapeHtml(casal.apelidos_ele)}</small></div>` : "";
    const apelidosEla = casal.apelidos_ela ? `<div><small>Apelidos ela: ${escapeHtml(casal.apelidos_ela)}</small></div>` : "";

    return `
      <button type="button" class="casal-busca-item" data-casal-json="${escapeHtml(JSON.stringify(casal))}">
        <div class="casal-busca-nome">
          ${escapeHtml(casal.nome_usual_ele)} e ${escapeHtml(casal.nome_usual_ela)}
        </div>
        <div class="casal-busca-meta">
          Ano: ${escapeHtml(casal.ano)} 
          ${casal.num_ecc ? " | ECC: " + escapeHtml(casal.num_ecc) : ""}
          ${casal.qtd_trabalhos ? " | Trabalhos: " + escapeHtml(casal.qtd_trabalhos) : ""}
        </div>
        <div class="casal-busca-completo">
          ${escapeHtml(casal.nome_completo_ele)} e ${escapeHtml(casal.nome_completo_ela)}
        </div>
        ${apelidosEle}
        ${apelidosEla}
        <div class="casal-busca-extra">
          ${casal.telefones ? "Tel: " + escapeHtml(casal.telefones) + "<br>" : ""}
          ${casal.endereco ? escapeHtml(casal.endereco) : ""}
        </div>
      </button>
    `;
  }

  function abrirLista(grupo, casais) {
    const popover = criarPopover();
    const body = qs(popover, ".casal-busca-body");

    body.innerHTML = casais.map(montarItem).join("");

    qsa(body, ".casal-busca-item").forEach(function (btn) {
      btn.addEventListener("click", function () {
        const casal = JSON.parse(btn.getAttribute("data-casal-json"));
        preencherCampos(grupo, casal);
        fecharPopover();
      });
    });

    popover.classList.add("is-open");
  }

  function mostrarMensagem(texto) {
    const popover = criarPopover();
    const body = qs(popover, ".casal-busca-body");

    body.innerHTML = `
      <div class="casal-busca-empty">
        ${escapeHtml(texto)}
      </div>
    `;

    popover.classList.add("is-open");
  }

  async function buscarCasal(grupo) {
    const inputEle = qs(grupo, "[data-casal-ele]");
    const inputEla = qs(grupo, "[data-casal-ela]");

    const nomeEle = inputEle ? inputEle.value.trim() : "";
    const nomeEla = inputEla ? inputEla.value.trim() : "";

    if (!nomeEle && !nomeEla) {
      mostrarMensagem("Informe pelo menos um nome para buscar.");
      return;
    }

    const url = new URL("/api/casais/buscar", window.location.origin);
    url.searchParams.set("nome_ele", nomeEle);
    url.searchParams.set("nome_ela", nomeEla);
    url.searchParams.set("limite", "50");

    try {
      const response = await fetch(url.toString(), {
        headers: { "Accept": "application/json" }
      });

      const data = await response.json();

      if (!response.ok || !data.ok) {
        mostrarMensagem(data.msg || "Nenhum casal encontrado.");
        return;
      }

      if (data.modo === "unico" && data.casal) {
        preencherCampos(grupo, data.casal);
        fecharPopover();
        return;
      }

      if (data.modo === "multiplo" && data.casais && data.casais.length) {
        abrirLista(grupo, data.casais);
        return;
      }

      mostrarMensagem("Nenhum casal encontrado.");
    } catch (err) {
      mostrarMensagem("Erro ao buscar casal.");
      console.error(err);
    }
  }

  function prepararGrupo(grupo) {
    const botao = qs(grupo, "[data-casal-buscar]");

    if (botao) {
      botao.addEventListener("click", function () {
        buscarCasal(grupo);
      });
    }

    qsa(grupo, "[data-casal-ele], [data-casal-ela]").forEach(function (input) {
      input.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          buscarCasal(grupo);
        }
      });
    });
  }

  function iniciar() {
    qsa(document, "[data-casal-grupo]").forEach(prepararGrupo);
  }

  document.addEventListener("DOMContentLoaded", iniciar);

  window.ECCCasalBusca = {
    buscarCasal: buscarCasal,
    preencherCampos: preencherCampos,
    fecharPopover: fecharPopover
  };
})();
