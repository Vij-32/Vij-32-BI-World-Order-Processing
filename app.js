const state = {
  orders: [],
  skuHsn: [],
  hsnPercent: [],
  companyGstinDefault: "33ABNCS8962N1ZE",
  assets: { logo: "", sign: "" },
  supabaseCfg: { url: "", anonKey: "" },
  supabaseClient: null,
  view: "orders",
  importPreview: null,
  importMapping: {},
  importHeaders: [],
  lastInvoiceSeq: 0,
  db: null,
  sortKey: null,
  sortDir: "asc",
  dashboardFilter: null,
  preventModalClose: false,
  modalContext: null
};

const REMOTE_API_BASE = (location.port === "8002") ? location.origin : null;

function applyEnvCfg() {
  try {
    const env = window.__ENV__ || {};
    const url = String(env.SUPABASE_URL || "").trim();
    const key = String(env.SUPABASE_ANON_KEY || "").trim();
    if (url && key) {
      state.supabaseCfg.url = url;
      state.supabaseCfg.anonKey = key;
    }
  } catch {}
}
const ORDERS_COLS = [
  { key: "vendorCode", label: "VendorCode" },
  { key: "uniqueId", label: "UniqueId" },
  { key: "purchaseOrder", label: "PurchaseOrder" },
  { key: "poDate", label: "PODate" },
  { key: "lineNbr", label: "LineNbr" },
  { key: "biPartNumber", label: "BIPartNumber" },
  { key: "productCode", label: "ProductCode" },
  { key: "productDescription", label: "ProductDescription" },
  { key: "uom", label: "UOM" },
  { key: "quantity", label: "Quantity" },
  { key: "unitPrice", label: "UnitPrice" },
  { key: "shipToName", label: "ShipToName" },
  { key: "shipToAddress1", label: "ShipToAddress1" },
  { key: "shipToAddress2", label: "ShipToAddress2" },
  { key: "shipToAddress3", label: "ShipToAddress3" },
  { key: "shipToAddress4", label: "ShipToAddress4" },
  { key: "city", label: "City" },
  { key: "state", label: "State" },
  { key: "postal", label: "Postal" },
  { key: "country", label: "Country" },
  { key: "phone", label: "Phone" },
  { key: "email", label: "Email" },
  { key: "accountNumber", label: "AccountNumber" },
  { key: "programNumber", label: "ProgramNumber" },
  { key: "comments", label: "Comments" },
  { key: "orderStatus", label: "OrderStatus" },
  { key: "biwpo", label: "BIWPO" },
  { key: "dispatchDate", label: "DispatchDate" },
  { key: "awb", label: "AWB" },
  { key: "courierName", label: "CourierName" },
  { key: "vendorInvoiceNumber", label: "VendorInvoiceNumber" },
  { key: "invoiceDate", label: "InvoiceDate" },
  { key: "poValue", label: "PoValue" },
  { key: "totalQuantity", label: "TotalQuantity" },
  { key: "totalPoValue", label: "TotalPoValue" },
  { key: "courierValue", label: "CourierValue" },
  { key: "totalCourier", label: "TotalCourier" },
  { key: "deliveryDate", label: "DeliveryDate" },
  { key: "weightKg", label: "Weight (in Kg)" },
  { key: "transportMode", label: "Mode of Transportation" },
  { key: "lbh", label: "LBH" },
  { key: "companyGstin", label: "Company GSTIN" },
  { key: "hsnCode", label: "HSN Code" },
  { key: "invoiceNumber", label: "InvoiceNumber" },
  { key: "unitPriceNoTax", label: "UnitPrice-NoTax" },
  { key: "netAmount", label: "NetAmount" },
  { key: "taxRate", label: "TaxRate" },
  { key: "taxType", label: "TaxType" },
  { key: "taxPercent", label: "Tax Percent" },
  { key: "taxAmount", label: "TaxAmount" },
  { key: "totalAmount", label: "TotalAmount" },
  { key: "mrp", label: "Mrp" },
  { key: "packedDate", label: "Packed Date" },
  { key: "lastUpdated", label: "Last Updated" }
];

async function dbOpen() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open("biworld", 1);
    req.onupgradeneeded = e => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains("orders")) {
        const os = db.createObjectStore("orders", { keyPath: "id", autoIncrement: true });
        os.createIndex("dedupeKey", "dedupeKey", { unique: true });
      }
      if (!db.objectStoreNames.contains("skuHsn")) {
        db.createObjectStore("skuHsn", { keyPath: "productKey" });
      }
      if (!db.objectStoreNames.contains("hsnPercent")) {
        db.createObjectStore("hsnPercent", { keyPath: "hsnCode" });
      }
      if (!db.objectStoreNames.contains("meta")) {
        db.createObjectStore("meta", { keyPath: "key" });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function dbGetAll(store) {
  return new Promise((resolve, reject) => {
    const tx = state.db.transaction(store, "readonly");
    const os = tx.objectStore(store);
    const req = os.getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function dbReplaceStore(store, rows) {
  return new Promise((resolve, reject) => {
    const tx = state.db.transaction(store, "readwrite");
    const os = tx.objectStore(store);
    const clearReq = os.clear();
    clearReq.onsuccess = () => {
      let pending = rows.length;
      if (!pending) resolve(true);
      rows.forEach(r => {
        if (store === "orders") {
          const dedupeKey = r.uniqueId ? `UID:${r.uniqueId}` : `PO:${r.purchaseOrder}|LN:${r.lineNbr}`;
          r.dedupeKey = dedupeKey;
        }
        if (store === "skuHsn") {
          r.productKey = String(r["ManufacturerModelNo"] || r["SupplierCode"] || "").trim().toLowerCase();
        }
        if (store === "hsnPercent") {
          r.hsnCode = String(r["HSN CODE"] || "").trim();
        }
        const putReq = os.put(r);
        putReq.onsuccess = () => { if (--pending === 0) resolve(true); };
        putReq.onerror = () => reject(putReq.error);
      });
    };
    clearReq.onerror = () => reject(clearReq.error);
  });
}

async function dbSetMeta(key, value) {
  return new Promise((resolve, reject) => {
    const tx = state.db.transaction("meta", "readwrite");
    const os = tx.objectStore("meta");
    const req = os.put({ key, value });
    req.onsuccess = () => resolve(true);
    req.onerror = () => reject(req.error);
  });
}

async function dbGetMeta(key) {
  return new Promise((resolve, reject) => {
    const tx = state.db.transaction("meta", "readonly");
    const os = tx.objectStore("meta");
    const req = os.get(key);
    req.onsuccess = () => resolve(req.result ? req.result.value : null);
    req.onerror = () => reject(req.error);
  });
}

async function initDBAndLoad() {
  let loaded = false;
  try {
    if (REMOTE_API_BASE) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 900);
      const resp = await fetch(REMOTE_API_BASE + "/api/state", { method: "GET", signal: ctrl.signal });
      clearTimeout(timer);
      if (resp.ok) {
        const data = await resp.json();
        state.orders = Array.isArray(data.orders) ? data.orders : [];
        state.skuHsn = Array.isArray(data.skuHsn) ? data.skuHsn : [];
        state.hsnPercent = Array.isArray(data.hsnPercent) ? data.hsnPercent : [];
        state.companyGstinDefault = data.companyGstinDefault || state.companyGstinDefault;
        state.lastInvoiceSeq = parseInt(data.lastInvoiceSeq || 0, 10) || 0;
        state.assets = data.assets || state.assets;
        if (data.supabaseCfg) state.supabaseCfg = data.supabaseCfg;
        loaded = true;
      }
    }
  } catch {}
  if (loaded) return;
  try {
    state.db = await dbOpen();
    const [orders, skuHsn, hsnPercent] = await Promise.all([
      dbGetAll("orders"),
      dbGetAll("skuHsn"),
      dbGetAll("hsnPercent")
    ]);
    state.orders = orders;
    state.skuHsn = skuHsn;
    state.hsnPercent = hsnPercent;
    const gstin = await dbGetMeta("companyGstinDefault");
    const invSeq = await dbGetMeta("lastInvoiceSeq");
    state.companyGstinDefault = gstin || state.companyGstinDefault;
    state.lastInvoiceSeq = invSeq ? parseInt(invSeq, 10) || 0 : state.lastInvoiceSeq;
    const logoMeta = await dbGetMeta("assetsLogo");
    const signMeta = await dbGetMeta("assetsSign");
    state.assets.logo = logoMeta || state.assets.logo || "";
    state.assets.sign = signMeta || state.assets.sign || "";
    const sbUrl = await dbGetMeta("supabaseUrl");
    const sbKey = await dbGetMeta("supabaseAnonKey");
    state.supabaseCfg.url = sbUrl || state.supabaseCfg.url || "";
    state.supabaseCfg.anonKey = sbKey || state.supabaseCfg.anonKey || "";
    if (!state.supabaseCfg.url || !state.supabaseCfg.anonKey) {
      try {
        const lsUrl = localStorage.getItem("supabaseUrl");
        const lsKey = localStorage.getItem("supabaseAnonKey");
        state.supabaseCfg.url = lsUrl || state.supabaseCfg.url || "";
        state.supabaseCfg.anonKey = lsKey || state.supabaseCfg.anonKey || "";
      } catch {}
    }
  } catch {
    const orders = localStorage.getItem("ordersData");
    const skuHsn = localStorage.getItem("skuHsnData");
    const hsnPercent = localStorage.getItem("hsnPercentData");
    const gstin = localStorage.getItem("companyGstinDefault");
    const inv = localStorage.getItem("lastInvoiceSeq");
    const assetsLogo = localStorage.getItem("assetsLogo");
    const assetsSign = localStorage.getItem("assetsSign");
    const sbUrl = localStorage.getItem("supabaseUrl");
    const sbKey = localStorage.getItem("supabaseAnonKey");
    state.orders = orders ? JSON.parse(orders) : [];
    state.skuHsn = skuHsn ? JSON.parse(skuHsn) : [];
    state.hsnPercent = hsnPercent ? JSON.parse(hsnPercent) : [];
    state.companyGstinDefault = gstin || state.companyGstinDefault;
    state.lastInvoiceSeq = inv ? parseInt(inv, 10) || 0 : 0;
    state.assets.logo = assetsLogo || state.assets.logo || "";
    state.assets.sign = assetsSign || state.assets.sign || "";
    state.supabaseCfg.url = sbUrl || state.supabaseCfg.url || "";
    state.supabaseCfg.anonKey = sbKey || state.supabaseCfg.anonKey || "";
  }
  if (state.supabaseCfg.url && state.supabaseCfg.anonKey && window.supabase) {
    state.supabaseClient = window.supabase.createClient(state.supabaseCfg.url, state.supabaseCfg.anonKey);
    try {
      const loadedSb = await supabaseLoadAll();
      if (loadedSb) return;
    } catch {}
  }
}

async function saveState() {
  if (state.db) {
    await Promise.all([
      dbReplaceStore("orders", state.orders),
      dbReplaceStore("skuHsn", state.skuHsn),
      dbReplaceStore("hsnPercent", state.hsnPercent),
      dbSetMeta("companyGstinDefault", state.companyGstinDefault),
      dbSetMeta("lastInvoiceSeq", String(state.lastInvoiceSeq)),
      dbSetMeta("assetsLogo", state.assets.logo || ""),
      dbSetMeta("assetsSign", state.assets.sign || ""),
      dbSetMeta("supabaseUrl", state.supabaseCfg.url || ""),
      dbSetMeta("supabaseAnonKey", state.supabaseCfg.anonKey || "")
    ]);
  } else {
    localStorage.setItem("ordersData", JSON.stringify(state.orders));
    localStorage.setItem("skuHsnData", JSON.stringify(state.skuHsn));
    localStorage.setItem("hsnPercentData", JSON.stringify(state.hsnPercent));
    localStorage.setItem("companyGstinDefault", state.companyGstinDefault);
    localStorage.setItem("lastInvoiceSeq", String(state.lastInvoiceSeq));
    localStorage.setItem("assetsLogo", state.assets.logo || "");
    localStorage.setItem("assetsSign", state.assets.sign || "");
    localStorage.setItem("supabaseUrl", state.supabaseCfg.url || "");
    localStorage.setItem("supabaseAnonKey", state.supabaseCfg.anonKey || "");
  }
  try {
    const payload = {
      orders: state.orders,
      skuHsn: state.skuHsn,
      hsnPercent: state.hsnPercent,
      companyGstinDefault: state.companyGstinDefault,
      lastInvoiceSeq: state.lastInvoiceSeq,
      assets: state.assets,
      supabaseCfg: state.supabaseCfg
    };
    if (REMOTE_API_BASE) {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 900);
      await fetch(REMOTE_API_BASE + "/api/state", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: ctrl.signal
      });
      clearTimeout(timer);
    }
  } catch {}
  if (state.supabaseClient) {
    try { await supabaseSaveAll(); } catch {}
  }
}

function renderAssets() {
  const logoInp = document.getElementById("asset-logo-input");
  const signInp = document.getElementById("asset-sign-input");
  const logoPrev = document.getElementById("asset-logo-preview");
  const signPrev = document.getElementById("asset-sign-preview");
  const saveBtn = document.getElementById("btn-save-assets");
  const sbUrlInp = document.getElementById("supabase-url");
  const sbKeyInp = document.getElementById("supabase-key");
  const sbStatus = document.getElementById("supabase-status");
  if (logoPrev) {
    logoPrev.src = state.assets.logo || "";
    logoPrev.style.display = state.assets.logo ? "block" : "none";
  }
  if (signPrev) {
    signPrev.src = state.assets.sign || "";
    signPrev.style.display = state.assets.sign ? "block" : "none";
  }
  if (sbUrlInp) sbUrlInp.value = state.supabaseCfg.url || "";
  if (sbKeyInp) sbKeyInp.value = state.supabaseCfg.anonKey || "";
  function readFileToDataUrl(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = e => resolve(e.target.result);
      reader.onerror = reject;
      reader.readAsDataURL(file);
    });
  }
  if (logoInp) {
    logoInp.onchange = async () => {
      const f = logoInp.files && logoInp.files[0];
      if (!f) return;
      const url = await readFileToDataUrl(f);
      state.assets.logo = url;
      if (logoPrev) { logoPrev.src = url; logoPrev.style.display = "block"; }
    };
  }
  if (signInp) {
    signInp.onchange = async () => {
      const f = signInp.files && signInp.files[0];
      if (!f) return;
      const url = await readFileToDataUrl(f);
      state.assets.sign = url;
      if (signPrev) { signPrev.src = url; signPrev.style.display = "block"; }
    };
  }
  if (saveBtn) {
    saveBtn.onclick = async () => {
      if (sbUrlInp && sbKeyInp) {
        state.supabaseCfg.url = sbUrlInp.value.trim();
        state.supabaseCfg.anonKey = sbKeyInp.value.trim();
        if (state.supabaseCfg.url && state.supabaseCfg.anonKey && window.supabase) {
          state.supabaseClient = window.supabase.createClient(state.supabaseCfg.url, state.supabaseCfg.anonKey);
        } else {
          state.supabaseClient = null;
        }
      }
      if (state.supabaseClient) {
        await supabaseLoadAll();
      }
      await saveState();
      alert("Assets saved");
    };
  }
  const testBtn = document.getElementById("btn-test-supabase");
  if (testBtn) {
    testBtn.onclick = async () => {
      const url = (sbUrlInp && sbUrlInp.value.trim()) || state.supabaseCfg.url;
      const key = (sbKeyInp && sbKeyInp.value.trim()) || state.supabaseCfg.anonKey;
      if (!url || !key || !window.supabase) {
        if (sbStatus) sbStatus.textContent = "Missing URL or Key";
        return;
      }
      const client = window.supabase.createClient(url, key);
      const { data, error } = await client.from("meta").select("key").limit(1);
      if (sbStatus) sbStatus.textContent = error ? "Connection failed" : "Connected";
    };
  }
}

async function supabaseLoadAll() {
  const c = state.supabaseClient;
  if (!c) return false;
  try {
    const m = await c.from("meta").select("key,value");
    if (m.error) throw m.error;
    const mp = {};
    (m.data || []).forEach(r => { mp[r.key] = r.value; });
    state.companyGstinDefault = mp["companyGstinDefault"] || state.companyGstinDefault;
    state.lastInvoiceSeq = mp["lastInvoiceSeq"] ? parseInt(mp["lastInvoiceSeq"],10) || 0 : state.lastInvoiceSeq;
    state.assets.logo = mp["assetsLogo"] || state.assets.logo || "";
    state.assets.sign = mp["assetsSign"] || state.assets.sign || "";
    const o = await c.from("orders").select("*");
    if (o.error) throw o.error;
    state.orders = (o.data || []).map(r => {
      const out = {};
      out.vendorCode = r.vendor_code ?? "";
      out.uniqueId = r.unique_id ?? "";
      out.purchaseOrder = r.purchase_order ?? "";
      out.poDate = r.po_date ?? "";
      out.lineNbr = r.line_nbr ?? "";
      out.biPartNumber = r.bi_part_number ?? "";
      out.productCode = r.product_code ?? "";
      out.productDescription = r.product_description ?? "";
      out.uom = r.uom ?? "";
      out.quantity = r.quantity ?? "";
      out.unitPrice = r.unit_price ?? "";
      out.shipToName = r.ship_to_name ?? "";
      out.shipToAddress1 = r.ship_to_address1 ?? "";
      out.shipToAddress2 = r.ship_to_address2 ?? "";
      out.shipToAddress3 = r.ship_to_address3 ?? "";
      out.shipToAddress4 = r.ship_to_address4 ?? "";
      out.city = r.city ?? "";
      out.state = r.state ?? "";
      out.postal = r.postal ?? "";
      out.country = r.country ?? "";
      out.phone = r.phone ?? "";
      out.email = r.email ?? "";
      out.accountNumber = r.account_number ?? "";
      out.programNumber = r.program_number ?? "";
      out.comments = r.comments ?? "";
      out.orderStatus = r.order_status ?? "";
      out.biwpo = r.biwpo ?? "";
      out.dispatchDate = r.dispatch_date ?? "";
      out.awb = r.awb ?? "";
      out.courierName = r.courier_name ?? "";
      out.vendorInvoiceNumber = r.vendor_invoice_number ?? "";
      out.invoiceDate = r.invoice_date ?? "";
      out.poValue = r.po_value ?? "";
      out.totalQuantity = r.total_quantity ?? "";
      out.totalPoValue = r.total_po_value ?? "";
      out.courierValue = r.courier_value ?? "";
      out.totalCourier = r.total_courier ?? "";
      out.deliveryDate = r.delivery_date ?? "";
      out.weightKg = r.weight_kg ?? "";
      out.transportMode = r.transport_mode ?? "";
      out.lbh = r.lbh ?? "";
      out.companyGstin = r.company_gstin ?? "";
      out.hsnCode = r.hsn_code ?? "";
      out.invoiceNumber = r.invoice_number ?? "";
      out.unitPriceNoTax = r.unit_price_no_tax ?? "";
      out.netAmount = r.net_amount ?? "";
      out.taxRate = r.tax_rate ?? "";
      out.taxType = r.tax_type ?? "";
      out.taxPercent = r.tax_percent ?? "";
      out.taxAmount = r.tax_amount ?? "";
      out.totalAmount = r.total_amount ?? "";
      out.mrp = r.mrp ?? "";
      out.packedDate = r.packed_date ?? "";
      out.lastUpdated = r.last_updated ?? "";
      return out;
    });
    const s = await c.from("sku_hsn").select("*");
    if (s.error) throw s.error;
    state.skuHsn = (s.data || []).map(r => {
      const out = {};
      out.productKey = r.product_key ?? "";
      out.SupplierCode = r.supplier_code ?? "";
      out.ManufacturerModelNo = r.manufacturer_model_no ?? "";
      out.HsnCode = r.hsn_code ?? "";
      out.Brand = r.brand ?? "";
      out.Category = r.category ?? "";
      out.ProductName = r.product_name ?? "";
      out.DescriptionofProduct = r.description_of_product ?? "";
      out.Category1 = r.category1 ?? "";
      out.Category2 = r.category2 ?? "";
      out.Category3 = r.category3 ?? "";
      out.MRP = r.mrp ?? "";
      out.PricetoBIinclofTaxes = r.price_to_bi_incl_taxes ?? "";
      out.CourierFinalPricetoBI = r.courier_final_price_to_bi ?? "";
      out.Cgst = r.cgst ?? "";
      out.Sgst = r.sgst ?? "";
      out.Igst = r.igst ?? "";
      out.Ugst = r.ugst ?? "";
      out.Weight = r.weight ?? "";
      out.FreightAmount1 = r.freight_amount1 ?? "";
      out.AccountType = r.account_type ?? "";
      out.ImageURL = r.image_url ?? "";
      out.ImageURL2 = r.image_url2 ?? "";
      out.ImageURL3 = r.image_url3 ?? "";
      out.ImageURL4 = r.image_url4 ?? "";
      out.CountryOfOrigin = r.country_of_origin ?? "";
      out.CurrentInventory = r.current_inventory ?? "";
      out.Status = r.status ?? "";
      out.CreationDate = r.creation_date ?? "";
      out.ProductType = r.product_type ?? "";
      return out;
    });
    const h = await c.from("hsn_percent").select("*");
    if (h.error) throw h.error;
    state.hsnPercent = (h.data || []).map(r => ({
      "HSN CODE": r.hsn_code ?? "",
      "PERCENT VALUE": r.percent_value ?? ""
    }));
    return true;
  } catch { return false; }
}

async function supabaseSaveAll() {
  const c = state.supabaseClient;
  if (!c) return false;
  const metaRows = [
    { key: "companyGstinDefault", value: state.companyGstinDefault || "" },
    { key: "lastInvoiceSeq", value: String(state.lastInvoiceSeq || 0) },
    { key: "assetsLogo", value: state.assets.logo || "" },
    { key: "assetsSign", value: state.assets.sign || "" }
  ];
  {
    const res = await c.from("meta").upsert(metaRows, { onConflict: "key" });
    if (res && res.error) console.error("Supabase upsert meta failed", res.error);
  }
  function computeDedupe(o) {
    const uid = String(o.uniqueId || "").trim();
    const po = String(o.purchaseOrder || "").trim();
    const ln = String(o.lineNbr || "").trim();
    if (uid) return `UID:${uid}`;
    if (po || ln) return `PO:${po}|LN:${ln}`;
    const key = [
      o.productCode,
      o.shipToName,
      o.invoiceNumber,
      o.biPartNumber,
      o.poDate
    ].map(v => String(v || "").trim()).join("|");
    return key ? "AUTO:" + hashStr(key) : null;
  }
  function hashStr(s) {
    let h = 5381;
    for (let i = 0; i < s.length; i++) h = ((h << 5) + h) + s.charCodeAt(i);
    return String(h >>> 0);
  }
  function num(v) {
    if (v === null || v === undefined || v === "") return null;
    const s = String(v).replace(/,/g, "").trim();
    const n = parseFloat(s);
    return isFinite(n) ? n : null;
  }
  const ordersRows = state.orders.map(o => ({
    dedupe_key: computeDedupe(o),
    vendor_code: o.vendorCode ?? null,
    unique_id: o.uniqueId ?? null,
    purchase_order: o.purchaseOrder ?? null,
    po_date: o.poDate ?? null,
    line_nbr: o.lineNbr ? parseInt(o.lineNbr,10) || null : null,
    bi_part_number: o.biPartNumber ?? null,
    product_code: o.productCode ?? null,
    product_description: o.productDescription ?? null,
    uom: o.uom ?? null,
    quantity: num(o.totalQuantity ?? o.quantity),
    unit_price: num(o.unitPrice),
    ship_to_name: o.shipToName ?? null,
    ship_to_address1: o.shipToAddress1 ?? null,
    ship_to_address2: o.shipToAddress2 ?? null,
    ship_to_address3: o.shipToAddress3 ?? null,
    ship_to_address4: o.shipToAddress4 ?? null,
    city: o.city ?? null,
    state: o.state ?? null,
    postal: o.postal ?? null,
    country: o.country ?? null,
    phone: o.phone ?? null,
    email: o.email ?? null,
    account_number: o.accountNumber ?? null,
    program_number: o.programNumber ?? null,
    comments: o.comments ?? null,
    order_status: o.orderStatus ?? null,
    biwpo: o.biwpo ?? null,
    dispatch_date: o.dispatchDate ?? null,
    awb: o.awb ?? null,
    courier_name: o.courierName ?? null,
    vendor_invoice_number: o.vendorInvoiceNumber ?? null,
    invoice_date: o.invoiceDate ?? null,
    po_value: num(o.poValue),
    total_quantity: num(o.totalQuantity),
    total_po_value: num(o.totalPoValue),
    courier_value: num(o.courierValue),
    total_courier: num(o.totalCourier),
    delivery_date: o.deliveryDate ?? null,
    weight_kg: num(o.weightKg),
    transport_mode: o.transportMode ?? null,
    lbh: o.lbh ?? null,
    company_gstin: o.companyGstin ?? null,
    hsn_code: o.hsnCode ?? null,
    invoice_number: o.invoiceNumber ?? null,
    unit_price_no_tax: num(o.unitPriceNoTax),
    net_amount: num(o.netAmount),
    tax_rate: num(o.taxRate),
    tax_type: o.taxType ?? null,
    tax_percent: num(o.taxPercent),
    tax_amount: num(o.taxAmount),
    total_amount: num(o.totalAmount),
    mrp: num(o.mrp),
    packed_date: o.packedDate ?? null,
    last_updated: o.lastUpdated ?? null
  })).filter(r => r.dedupe_key);
  if (ordersRows.length) {
    const res = await c.from("orders").upsert(ordersRows, { onConflict: "dedupe_key" });
    if (res && res.error) console.error("Supabase upsert orders failed", res.error);
  }
  function productKeyOf(row) {
    const mk = String(row.productKey || "").trim().toLowerCase();
    const mm = String(row.ManufacturerModelNo || "").trim().toLowerCase();
    const sc = String(row.SupplierCode || "").trim().toLowerCase();
    return mk || mm || sc || null;
  }
  const skuRows = state.skuHsn.map(r => ({
    product_key: productKeyOf(r),
    supplier_code: r.SupplierCode ?? null,
    manufacturer_model_no: r.ManufacturerModelNo ?? null,
    hsn_code: r.HsnCode ?? null,
    brand: r.Brand ?? null,
    category: r.Category ?? null,
    product_name: r.ProductName ?? null,
    description_of_product: r.DescriptionofProduct ?? null,
    category1: r.Category1 ?? null,
    category2: r.Category2 ?? null,
    category3: r.Category3 ?? null,
    mrp: num(r.MRP),
    price_to_bi_incl_taxes: num(r.PricetoBIinclofTaxes),
    courier_final_price_to_bi: num(r.CourierFinalPricetoBI),
    cgst: num(r.Cgst),
    sgst: num(r.Sgst),
    igst: num(r.Igst),
    ugst: num(r.Ugst),
    weight: num(r.Weight),
    freight_amount1: num(r.FreightAmount1),
    account_type: r.AccountType ?? null,
    image_url: r.ImageURL ?? null,
    image_url2: r.ImageURL2 ?? null,
    image_url3: r.ImageURL3 ?? null,
    image_url4: r.ImageURL4 ?? null,
    country_of_origin: r.CountryOfOrigin ?? null,
    current_inventory: num(r.CurrentInventory),
    status: r.Status ?? null,
    creation_date: r.CreationDate ?? null,
    product_type: r.ProductType ?? null
  })).filter(r => r.product_key);
  if (skuRows.length) {
    const res = await c.from("sku_hsn").upsert(skuRows, { onConflict: "product_key" });
    if (res && res.error) console.error("Supabase upsert sku_hsn failed", res.error);
  }
  const hsnRows = state.hsnPercent.map(r => ({ hsn_code: String(r["HSN CODE"] || r.hsnCode || "").trim(), percent_value: r["PERCENT VALUE"] || r.percent_value || null }));
  {
    const res = await c.from("hsn_percent").upsert(hsnRows, { onConflict: "hsn_code" });
    if (res && res.error) console.error("Supabase upsert hsn_percent failed", res.error);
  }
  return true;
}

function $(sel) { return document.querySelector(sel); }

function switchView(v) {
  state.view = v;
  showLoading();
  document.querySelectorAll(".view").forEach(s => s.classList.add("hidden"));
  $("#view-" + v).classList.remove("hidden");
  if (v !== "dashboard") state.dashboardFilter = null;
  if (v === "orders") renderOrders();
  if (v === "update-orders") renderPendingOrders();
  if (v === "sku-hsn") renderSkuHsn();
  if (v === "hsn-percent") renderHsnPercent();
  if (v === "assets") renderAssets();
  if (v === "dashboard") renderDashboard();
  setTimeout(hideLoading, 200);
}

function normalizeHeader(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function bestMatch(header, labels) {
  const n = normalizeHeader(header);
  const labelNorms = labels.map(l => ({ l, n: normalizeHeader(l) }));
  for (const item of labelNorms) {
    if (item.n === n) return item.l;
  }
  for (const item of labelNorms) {
    if (item.n.includes(n) || n.includes(item.n)) return item.l;
  }
  const synonyms = [
    ["hsncode", "hsncode", "hsn"],
    ["productcode", "manufacturermodelno", "bipartnumber"],
    ["uom", "unitofmeasure"],
    ["quantity", "qty"],
    ["unitprice", "price", "unitpriceincltax", "unitpriceexcltax"],
    ["invoice", "invoicenumber"]
  ];
  for (const syn of synonyms) {
    if (syn.includes(n)) {
      for (const item of labelNorms) {
        if (syn.includes(item.n)) return item.l;
      }
    }
  }
  return null;
}

function renderOrders() {
  const thead = $("#orders-thead");
  const tbody = $("#orders-tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  const trh = document.createElement("tr");
  const thSel = document.createElement("th");
  thSel.textContent = "";
  thSel.className = "row-select";
  trh.appendChild(thSel);
  const thAct = document.createElement("th");
  thAct.textContent = "Actions";
  thAct.className = "row-actions";
  trh.appendChild(thAct);
  for (const c of ORDERS_COLS) {
    const th = document.createElement("th");
    th.style.display = "flex";
    th.style.alignItems = "center";
    th.style.gap = "6px";
    const lab = document.createElement("span");
    lab.textContent = c.label;
    const ind = document.createElement("span");
    ind.style.fontSize = "12px";
    ind.style.opacity = "0.8";
    ind.textContent = (state.sortKey === c.key) ? (state.sortDir === "asc" ? "▲" : "▼") : "";
    th.appendChild(lab);
    th.appendChild(ind);
    th.style.cursor = "pointer";
    th.addEventListener("click", () => {
      showLoading();
      const prevKey = state.sortKey;
      const key = c.key;
      if (prevKey === key) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = key;
        state.sortDir = "asc";
      }
      setTimeout(() => { renderOrders(); hideLoading(); }, 50);
    });
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  const rows = getFilteredAndSortedOrders();
  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    tr.addEventListener("click", (e) => {
      const tag = e.target.tagName.toLowerCase();
      if (tag === "input" || tag === "button") return;
      showOrderDetails(row);
    });
    const tdSel = document.createElement("td");
    tdSel.className = "row-select";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !!row.__selected;
    cb.addEventListener("change", () => {
      row.__selected = cb.checked;
      saveState();
      updateUpdateSelectedButton();
    });
    tdSel.appendChild(cb);
    tr.appendChild(tdSel);
    const tdAct = document.createElement("td");
    tdAct.className = "row-actions";
    const delBtn = document.createElement("button");
    delBtn.className = "icon-btn";
    delBtn.textContent = "🗑";
    delBtn.title = "Delete";
    delBtn.addEventListener("click", async () => confirmDelete([row]));
    tdAct.appendChild(delBtn);
    tr.appendChild(tdAct);
    for (const c of ORDERS_COLS) {
      const td = document.createElement("td");
      const v = row[c.key] ?? "";
      td.textContent = v;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  });
}

function getFilteredAndSortedOrders() {
  const rows = state.orders.slice();
  const q = (state.searchQuery || "").toLowerCase().trim();
  const filtered = q ? rows.filter(r => {
    return ORDERS_COLS.some(c => String(r[c.key] ?? "").toLowerCase().includes(q));
  }) : rows;
  const key = state.sortKey;
  const dir = state.sortDir === "desc" ? -1 : 1;
  if (!key) return filtered;
  filtered.sort((a, b) => {
    const va = a[key] ?? "";
    const vb = b[key] ?? "";
    const na = parseFloat(va);
    const nb = parseFloat(vb);
    const aNum = !isNaN(na) && String(va).trim() !== "";
    const bNum = !isNaN(nb) && String(vb).trim() !== "";
    if (aNum && bNum) {
      return (na - nb) * dir;
    }
    const da = Date.parse(va);
    const db = Date.parse(vb);
    const aDate = !isNaN(da);
    const bDate = !isNaN(db);
    if (aDate && bDate) {
      return (da - db) * dir;
    }
    return String(va).localeCompare(String(vb)) * dir;
  });
  return filtered;
}

function showOrderDetails(order) {
  const body = document.createElement("div");
  const grid = document.createElement("div");
  grid.className = "form-grid";
  ORDERS_COLS.forEach(c => {
    const f = document.createElement("div");
    f.className = "form-field";
    const lab = document.createElement("label");
    lab.textContent = c.label;
    const val = document.createElement("input");
    val.readOnly = true;
    val.value = order[c.key] ?? "";
    f.appendChild(lab);
    f.appendChild(val);
    grid.appendChild(f);
  });
  body.appendChild(grid);
  showModal("Order Details", body);
}

function renderPendingOrders() {
  const thead = $("#pending-orders-thead");
  const tbody = $("#pending-orders-tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  const trh = document.createElement("tr");
  for (const c of ORDERS_COLS) {
    const th = document.createElement("th");
    th.textContent = c.label;
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  state.orders
    .filter(r => String(r.orderStatus || "").toLowerCase() === "pending")
    .forEach(row => {
      const tr = document.createElement("tr");
      tr.addEventListener("click", (e) => {
        const tag = e.target.tagName.toLowerCase();
        if (tag === "input" || tag === "button") return;
        buildUpdateDialog(row);
      });
      for (const c of ORDERS_COLS) {
        const td = document.createElement("td");
        const v = row[c.key] ?? "";
        td.textContent = v;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    });
}

function renderSkuHsn() {
  const thead = $("#sku-hsn-thead");
  const tbody = $("#sku-hsn-tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  const cols = [
    "SupplierCode","ManufacturerModelNo","HsnCode","Brand","Category","ProductName","DescriptionofProduct","Category1","Category2","Category3","MRP","PricetoBIinclofTaxes","CourierFinalPricetoBI","Cgst","Sgst","Igst","Ugst","Weight","FreightAmount1","AccountType","ImageURL","ImageURL2","ImageURL3","ImageURL4","CountryOfOrigin","CurrentInventory","Status","CreationDate","ProductType"
  ];
  const trh = document.createElement("tr");
  cols.forEach(h => {
    const th = document.createElement("th");
    th.textContent = h;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  state.skuHsn.forEach(row => {
    const tr = document.createElement("tr");
    cols.forEach(h => {
      const td = document.createElement("td");
      td.textContent = row[h] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function renderHsnPercent() {
  const thead = $("#hsn-percent-thead");
  const tbody = $("#hsn-percent-tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  const cols = ["HSN CODE","PERCENT VALUE"];
  const trh = document.createElement("tr");
  cols.forEach(h => {
    const th = document.createElement("th");
    th.textContent = h;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  state.hsnPercent.forEach(row => {
    const tr = document.createElement("tr");
    cols.forEach(h => {
      const td = document.createElement("td");
      td.textContent = row[h] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function renderSpillboxIm() {
  const thead = $("#spillbox-thead");
  const tbody = $("#spillbox-tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";
  const cols = Object.keys(state.spillboxIm[0] || {}).filter(k => k !== "id");
  if (cols.length) {
    const trh = document.createElement("tr");
    cols.forEach(h => {
      const th = document.createElement("th");
      th.textContent = h;
      trh.appendChild(th);
    });
    thead.appendChild(trh);
  }
  state.spillboxIm.forEach(row => {
    const tr = document.createElement("tr");
    (cols.length ? cols : Object.keys(row)).forEach(h => {
      if (h === "id") return;
      const td = document.createElement("td");
      td.textContent = row[h] ?? "";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

function showModal(title, bodyNode) {
  $("#modal-title").textContent = title;
  const body = $("#modal-body");
  body.innerHTML = "";
  body.appendChild(bodyNode);
  $("#modal").classList.remove("hidden");
}

function hideModal() {
  state.preventModalClose = false;
  $("#modal").classList.add("hidden");
  $("#modal-prev").classList.add("hidden");
  $("#modal-next").classList.add("hidden");
  $("#modal-confirm").classList.add("hidden");
}

function parseFile(file) {
  return new Promise((resolve, reject) => {
    const ext = file.name.split(".").pop().toLowerCase();
    const reader = new FileReader();
    if (ext === "csv") {
      Papa.parse(file, {
        header: true,
        dynamicTyping: false,
        skipEmptyLines: true,
        complete: res => resolve({ rows: res.data, headers: res.meta.fields }),
        error: err => reject(err)
      });
    } else {
      reader.onload = e => {
        const data = new Uint8Array(e.target.result);
        const wb = XLSX.read(data, { type: "array" });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const json = XLSX.utils.sheet_to_json(ws, { defval: "" });
        const headers = Object.keys(json[0] || {});
        resolve({ rows: json, headers });
      };
      reader.onerror = reject;
      reader.readAsArrayBuffer(file);
    }
  });
}

function buildMappingUI(headers) {
  const labels = ORDERS_COLS.map(c => c.label);
  const container = document.createElement("div");
  const actions = document.createElement("div");
  actions.className = "sticky-actions";
  const addBtnTop = document.createElement("button");
  addBtnTop.textContent = "Add Order";
  addBtnTop.addEventListener("click", () => {
    const btn = document.getElementById("modal-confirm");
    if (btn) btn.click();
  });
  actions.appendChild(addBtnTop);
  container.appendChild(actions);
  const info = document.createElement("div");
  info.className = "warning";
  info.textContent = "Map uploaded file headers to Orders columns. Preset best matches can be changed.";
  container.appendChild(info);
  const grid = document.createElement("div");
  grid.className = "mapping-grid";
  labels.forEach(label => {
    const row = document.createElement("div");
    row.className = "mapping-row";
    const lab = document.createElement("label");
    lab.textContent = label;
    const sel = document.createElement("select");
    const optNone = document.createElement("option");
    optNone.value = "";
    optNone.textContent = "—";
    sel.appendChild(optNone);
    headers.forEach(h => {
      const opt = document.createElement("option");
      opt.value = h;
      opt.textContent = h;
      sel.appendChild(opt);
    });
    const preset = bestMatch(label, headers);
    if (preset) sel.value = preset;
    sel.addEventListener("change", () => {
      state.importMapping[label] = sel.value;
    });
    state.importMapping[label] = sel.value || "";
    row.appendChild(lab);
    row.appendChild(sel);
    grid.appendChild(row);
  });
  container.appendChild(grid);
  const footerTip = document.createElement("div");
  footerTip.style.marginTop = "8px";
  footerTip.style.color = "#cbd5e1";
  footerTip.textContent = "Click Add Order to review and import.";
  container.appendChild(footerTip);
  $("#modal-prev").classList.add("hidden");
  $("#modal-next").classList.add("hidden");
  $("#modal-confirm").classList.remove("hidden");
  $("#modal-confirm").textContent = "Add Order";
  state.modalContext = "import";
  return container;
}

function buildImportWarningUI() {
  const container = document.createElement("div");
  const warn = document.createElement("div");
  warn.className = "warning";
  warn.textContent = "Orders will be imported with respect to the matched columns.";
  container.appendChild(warn);
  const summary = document.createElement("div");
  summary.id = "import-summary";
  summary.style.color = "#cbd5e1";
  container.appendChild(summary);
  $("#modal-prev").classList.remove("hidden");
  $("#modal-next").classList.remove("hidden");
  $("#modal-confirm").classList.add("hidden");
  $("#modal-prev").textContent = "Previous";
  $("#modal-next").textContent = "Next";
  return container;
}

function applyImport(mapping, rows) {
  const labelToKey = Object.fromEntries(ORDERS_COLS.map(c => [c.label, c.key]));
  const existingKeys = new Set(
    state.orders.map(r => r.uniqueId ? `UID:${r.uniqueId}` : `PO:${r.purchaseOrder}|LN:${r.lineNbr}`)
  );
  let added = 0;
  let skipped = 0;
  const imported = [];
  for (const row of rows) {
    const obj = {};
    for (const [label, fileHeader] of Object.entries(mapping)) {
      const key = labelToKey[label];
      const val = fileHeader ? row[fileHeader] : "";
      let v = val ?? "";
      if (key === "phone") v = normalizePhone(v);
      obj[key] = v;
    }
    if (!obj.orderStatus) obj.orderStatus = "Pending";
    const dedupeKey = obj.uniqueId ? `UID:${obj.uniqueId}` : `PO:${obj.purchaseOrder}|LN:${obj.lineNbr}`;
    if (existingKeys.has(dedupeKey)) {
      skipped++;
      continue;
    }
    imported.push(obj);
    existingKeys.add(dedupeKey);
    added++;
  }
  state.orders = state.orders.concat(imported);
  saveState();
  return { added, skipped };
}

function normalizePhone(v) {
  const s = String(v || "");
  const plus = s.trim().startsWith("+");
  const digits = s.replace(/[^0-9]/g, "");
  return plus ? "+" + digits : digits;
}

function updateUpdateSelectedButton() {
  const anyOrdersSelected = state.orders.some(r => r.__selectedPending || r.__selected);
  $("#btn-update-selected").disabled = !anyOrdersSelected;
}

function updatePrintButtons() {
  const anyOrdersSelected = state.orders.some(r => r.__selected);
  const btnLabel = document.getElementById("btn-print-label");
  const btnInvoice = document.getElementById("btn-print-invoice");
  const btnBoth = document.getElementById("btn-print-both");
  if (btnLabel) btnLabel.disabled = !anyOrdersSelected;
  if (btnInvoice) btnInvoice.disabled = !anyOrdersSelected;
  if (btnBoth) btnBoth.disabled = !anyOrdersSelected;
}

function buildLabelHTML(order) {
  const po = String(order.purchaseOrder || "").trim();
  const poDate = String(order.poDate || "").trim();
  const shipLines = [
    order.shipToName, order.shipToAddress1, order.shipToAddress2,
    order.shipToAddress3, order.shipToAddress4,
    [order.city, order.state, order.postal].filter(Boolean).join(", "),
    order.country ? String(order.country) : "",
    order.phone ? "Phone: " + normalizePhone(order.phone) : "",
    order.email ? "Email: " + order.email : ""
  ].filter(s => String(s || "").trim() !== "").join("<br>");
  const desc = String(order.productDescription || "").trim();
  const code = String(order.productCode || "").trim();
  const hsn = String(order.hsnCode || "").trim();
  const qty = String(order.totalQuantity || order.quantity || "").trim();
  const gstin = String(state.companyGstinDefault || "").trim();
  const seller = [
    "Spillbox Innovation Private Limited",
    "2/852, Manapakkam-Mugalivakkam Main Road,",
    "Chennai, Tamil Nadu 600125",
    "Phone: 89392 97454",
    "GSTIN: " + gstin
  ].join("<br>");
  return `
  <div class="page">
    <div class="outer">
      <div class="top-header">
        <h2>Shipping Label</h2>
        ${state.assets.logo ? `<img class="logo" src="${state.assets.logo}" alt="logo">` : ``}
      </div>
      <div class="barcode">*${po}*</div>
      <div class="section">
        <div class="bold">Ship To:</div>
        ${shipLines}
      </div>
      <div class="section">
        <div class="info-line">Order Number: ${po}</div>
        <div class="info-line">PO Date: ${poDate}</div>
      </div>
      <div class="section">
        <div class="bold">From:</div>
        ${seller}
      </div>
      <div class="section">
        <table>
          <thead>
            <tr>
              <th>Description</th><th>Product Code</th><th>HSN</th><th>Qty</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${desc}</td><td>${code}</td><td>${hsn}</td><td>${qty}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>`;
}

function buildInvoiceHTML(order, opts = { withSign: false }) {
  const po = String(order.purchaseOrder || "").trim();
  const poDate = String(order.poDate || "").trim();
  const invoiceNo = String(order.invoiceNumber || "").trim();
  const invoiceDate = String(order.invoiceDate || "").trim();
  const shipLines = [
    order.shipToName, order.shipToAddress1, order.shipToAddress2,
    order.shipToAddress3, order.shipToAddress4,
    [order.city, order.state, order.postal].filter(Boolean).join(", "),
    order.country ? String(order.country) : "",
    order.phone ? "Phone: " + normalizePhone(order.phone) : "",
    order.email ? "Email: " + order.email : ""
  ].filter(s => String(s || "").trim() !== "").join("<br>");
  const billing = [
    "BI WORLDWIDE INDIA PRIVATE LIMITED",
    "No 28 Ulsoor Road, Next to Nilgiris",
    "Bangalore, Karnataka",
    "Pincode: 560042",
    "GSTIN: 29AAECB5878L1ZY"
  ].join("<br>");
  const gstin = String(state.companyGstinDefault || "").trim();
  const seller = [
    "Spillbox Innovation Private Limited",
    "2/852, Manapakkam-Mugalivakkam Main Road,",
    "Chennai, Tamil Nadu 600125",
    "Phone: 89392 97454",
    "GSTIN: " + gstin
  ].join("<br>");
  const desc = String(order.productDescription || "").trim();
  const code = String(order.productCode || "").trim();
  const hsn = String(order.hsnCode || "").trim();
  const unitNoTax = String(order.unitPriceNoTax || "").trim();
  const qty = String(order.totalQuantity || order.quantity || "").trim();
  const netAmount = String(order.netAmount || "").trim();
  const taxRate = String(order.taxRate || "").trim();
  const taxType = String(order.taxType || "").trim();
  const taxAmount = String(order.taxAmount || "").trim();
  const totalAmount = String(order.totalAmount || "").trim();
  return `
  <div class="page">
    <div class="outer">
      <div class="top-header">
        <h2>Tax Invoice</h2>
        ${state.assets.logo ? `<img class="logo" src="${state.assets.logo}" alt="logo">` : ``}
      </div>
      <div class="section">
        <div class="bold">Billing Address:</div>
        ${billing}
      </div>
      <div class="section">
        <div class="bold">Shipping Address:</div>
        ${shipLines}
      </div>
      <div class="section">
        <div class="bold">Sold By:</div>
        ${seller}
      </div>
      <div class="section">
        <div class="info-line">Order Number: ${po} | PO Date: ${poDate}</div>
        <div class="info-line">Invoice Number: ${invoiceNo} | Invoice Date: ${invoiceDate}</div>
      </div>
      <div class="section">
        <table>
          <thead>
            <tr>
              <th>Description</th><th>Product Code</th><th>HSN</th><th>UnitPrice-NoTax</th><th>Qty</th><th>Net Amount</th><th>Tax Rate</th><th>Tax Type</th><th>Tax Amount</th><th>Total</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${desc}</td><td>${code}</td><td>${hsn}</td><td>${unitNoTax}</td><td>${qty}</td><td>${netAmount}</td><td>${taxRate}</td><td>${taxType}</td><td>${taxAmount}</td><td>${totalAmount}</td>
            </tr>
          </tbody>
        </table>
      </div>
      ${opts.withSign
        ? `<div class="sign-block"><div class="bold">Authorized Signature</div>${state.assets.sign ? `<img class="sign-img" src="${state.assets.sign}" alt="signature">` : ``}</div>`
        : `<div class="footer-note">Auto Generated Invoice, Signature Not Required</div>`
      }
    </div>
  </div>`;
}

function printPagesForSelected(mode) {
  const selected = state.orders.filter(r => r.__selected);
  if (!selected.length) return;
  const pages = [];
  if (mode === "label") {
    selected.forEach(o => { pages.push(buildLabelHTML(o)); });
  } else if (mode === "invoice") {
    const withSign = state.modalContext && state.modalContext.invoiceWithSign ? true : false;
    selected.forEach(o => { pages.push(buildInvoiceHTML(o, { withSign })); });
  } else {
    const withSign = state.modalContext && state.modalContext.invoiceWithSign ? true : false;
    selected.forEach(o => { pages.push(buildLabelHTML(o)); pages.push(buildInvoiceHTML(o, { withSign })); });
  }
  const doc = `
  <!doctype html>
  <html>
    <head>
      <meta charset="utf-8" />
      <title>Print</title>
      <link href="https://fonts.googleapis.com/css2?family=Libre+Barcode+39&display=swap" rel="stylesheet">
      <style>
        @page { size: A4; margin: 0; }
        @media print { .page { page-break-after: always; } }
        html, body { margin: 0; padding: 0; }
        .page { width: 210mm; min-height: 297mm; display: flex; align-items: flex-start; justify-content: center; }
        .outer { width: 190mm; min-height: 277mm; border: 2px solid black; padding: 15mm; box-sizing: border-box; margin: 10mm auto 0 auto; font-family: Arial, sans-serif; font-size: 14px; }
        .top-header { margin-bottom: 12px; }
        .top-header h2 { margin: 0; font-size: 24px; }
        .top-header .logo { position: absolute; right: 15mm; top: 15mm; height: 24mm; }
        .outer { position: relative; }
        .section { margin-bottom: 12px; }
        .info-line { margin-bottom: 6px; }
        .bold { font-weight: 700; margin-bottom: 6px; }
        table { width: 100%; border-collapse: collapse; table-layout: fixed; }
        th, td { border: 1px solid black; padding: 6px; text-align: left; font-weight: 600; font-size: 12px; word-wrap: break-word; }
        th { background: #f2f2f2; }
        .barcode { font-family: 'Libre Barcode 39', cursive; font-size: 72px; text-align: left; margin: 10px 0; }
        .footer-note { margin-top: 12px; text-align: center; font-size: 12px; font-weight: 600; }
        .sign-block { position: absolute; right: 20mm; bottom: 20mm; text-align: right; }
        .sign-img { height: 20mm; display: block; margin-top: 4px; margin-left: auto; }
      </style>
    </head>
    <body>
      ${pages.join("\n")}
      <script>window.addEventListener('load', () => { window.print(); });</script>
    </body>
  </html>`;
  const w = window.open("", "_blank");
  if (!w) return;
  w.document.open();
  w.document.write(doc);
  w.document.close();
}

function askInvoiceOptions(callbackForSign) {
  const container = document.createElement("div");
  const label1 = document.createElement("p");
  label1.textContent = "Choose Invoice Option:";
  const btnWith = document.createElement("button");
  btnWith.textContent = "With Sign";
  const btnWithout = document.createElement("button");
  btnWithout.textContent = "Without Sign";
  btnWith.style.marginRight = "8px";
  container.appendChild(label1);
  container.appendChild(btnWith);
  container.appendChild(btnWithout);
  btnWith.onclick = () => {
    state.modalContext = { invoiceWithSign: true };
    hideModal();
    callbackForSign(true);
  };
  btnWithout.onclick = () => {
    state.modalContext = { invoiceWithSign: false };
    hideModal();
    callbackForSign(false);
  };
  showModal("Invoice Options", container);
}
function computeAutoFetch(order, opts = { assignInvoice: true }) {
  order.poValue = parseFloat(order.unitPrice || 0) || 0;
  order.totalQuantity = parseFloat(order.quantity || 0) || 0;
  order.totalPoValue = Number(order.poValue) * Number(order.totalQuantity);
  order.biwpo = order.purchaseOrder || "";
  order.companyGstin = state.companyGstinDefault;
  const prodKey = String(order.productCode || "").trim().toLowerCase();
  let hsn = "";
  let mrp = "";
  for (const r of state.skuHsn) {
    const model = String(r["ManufacturerModelNo"] || "").trim().toLowerCase();
    const sup = String(r["SupplierCode"] || "").trim().toLowerCase();
    if (model && model === prodKey) {
      hsn = r["HsnCode"] || hsn;
      mrp = r["MRP"] || mrp;
      break;
    }
    if (!hsn && sup && sup === prodKey) {
      hsn = r["HsnCode"] || hsn;
      mrp = r["MRP"] || mrp;
    }
  }
  order.hsnCode = hsn || order.hsnCode || "";
  order.mrp = mrp || order.mrp || "";
  let rate = "";
  for (const r of state.hsnPercent) {
    const code = String(r["HSN CODE"] || "").trim();
    if (code && code === order.hsnCode) {
      rate = r["PERCENT VALUE"];
      break;
    }
  }
  order.taxRate = rate || order.taxRate || "";
  order.taxType = "IGST";
  const taxPct = (parseFloat(order.taxRate || 0) || 0) / 100;
  order.taxPercent = taxPct;
  const up = parseFloat(order.unitPrice || 0) || 0;
  const qty = parseFloat(order.quantity || 0) || 0;
  const unitNoTax = up / (1 + taxPct);
  order.unitPriceNoTax = round2(unitNoTax);
  order.netAmount = round2(unitNoTax * qty);
  order.taxAmount = round2(order.netAmount * taxPct);
  order.totalAmount = round2(order.netAmount + order.taxAmount);
  if (opts.assignInvoice && !order.invoiceNumber) {
    order.invoiceNumber = nextInvoiceNumber();
  }
  order.vendorInvoiceNumber = order.invoiceNumber;
  return order;
}

function round2(n) {
  return Math.round((Number(n) + Number.EPSILON) * 100) / 100;
}

function nextInvoiceNumber() {
  const fy = fiscalYearString();
  const prefix = "SP/BI/" + fy + "/";
  let maxSeq = 0;
  state.orders.forEach(o => {
    const inv = String(o.invoiceNumber || o.vendorInvoiceNumber || "");
    if (inv.startsWith(prefix)) {
      const n = parseInt(inv.slice(prefix.length), 10);
      if (!isNaN(n)) maxSeq = Math.max(maxSeq, n);
    }
  });
  const metaKey = "lastInvoiceSeq_" + fy;
  const stored = localStorage.getItem(metaKey);
  const storedSeq = stored ? parseInt(stored, 10) || 0 : 0;
  maxSeq = Math.max(maxSeq, storedSeq);
  const next = maxSeq + 1;
  localStorage.setItem(metaKey, String(next));
  return prefix + String(next);
}

function fiscalYearString() {
  const d = new Date();
  const y = d.getFullYear(); // e.g. 2025
  const m = d.getMonth() + 1; // 1-12
  const startYear = m >= 4 ? y % 100 : (y - 1) % 100;
  const endYear = (startYear + 1) % 100;
  return String(startYear).padStart(2, "0") + "-" + String(endYear).padStart(2, "0");
}

async function onReady() {
  applyEnvCfg();
  await initDBAndLoad();
  hideModal();
  document.querySelectorAll(".nav-btn").forEach(b => {
    b.addEventListener("click", () => {
      hideModal();
      switchView(b.dataset.view);
    });
  });

  const toggleAll = document.getElementById("toggle-select-all");
  if (toggleAll) {
    toggleAll.addEventListener("change", e => {
      const checked = e.target.checked;
      state.orders.forEach(r => r.__selected = checked);
      saveState();
      renderOrders();
      updateUpdateSelectedButton();
      updatePrintButtons();
    });
  }
  const clearSelBtn = document.getElementById("clear-selection");
  if (clearSelBtn) {
    clearSelBtn.addEventListener("click", () => {
      state.orders.forEach(r => { r.__selected = false; r.__selectedPending = false; });
      saveState();
      renderOrders();
      renderPendingOrders();
      updateUpdateSelectedButton();
      updatePrintButtons();
    });
  }
  const delSelected = document.getElementById("delete-selected");
  if (delSelected) {
    delSelected.addEventListener("click", async () => {
      const selected = state.orders.filter(r => r.__selected);
      confirmDelete(selected);
    });
  }
  const searchInput = document.getElementById("orders-search");
  if (searchInput) {
    searchInput.addEventListener("input", e => {
      state.searchQuery = e.target.value;
      renderOrders();
    });
  }
  const clearSearch = document.getElementById("clear-search");
  if (clearSearch) {
    clearSearch.addEventListener("click", () => {
      state.searchQuery = "";
      const input = document.getElementById("orders-search");
      if (input) input.value = "";
      renderOrders();
    });
  }
  const exportBtn = document.getElementById("export-csv");
  if (exportBtn) {
    exportBtn.addEventListener("click", () => {
      const rows = getFilteredAndSortedOrders();
      const header = ORDERS_COLS.map(c => c.label);
      const keys = ORDERS_COLS.map(c => c.key);
      const csv = [header.join(",")].concat(
        rows.map(r => keys.map(k => escapeCsv(r[k])).join(","))
      ).join("\n");
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "orders.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    });
  }
  const btnLabel = document.getElementById("btn-print-label");
  const btnInvoice = document.getElementById("btn-print-invoice");
  const btnBoth = document.getElementById("btn-print-both");
  if (btnLabel) btnLabel.addEventListener("click", () => printPagesForSelected("label"));
  if (btnInvoice) btnInvoice.addEventListener("click", () => {
    askInvoiceOptions(() => printPagesForSelected("invoice"));
  });
  if (btnBoth) btnBoth.addEventListener("click", () => {
    const container = document.createElement("div");
    const label1 = document.createElement("p");
    label1.textContent = "Choose Label & Invoice Option:";
    const btnWith = document.createElement("button");
    btnWith.textContent = "Label + Signed Invoice";
    const btnWithout = document.createElement("button");
    btnWithout.textContent = "Label + Non-Signed Invoice";
    btnWith.style.marginRight = "8px";
    container.appendChild(label1);
    container.appendChild(btnWith);
    container.appendChild(btnWithout);
    btnWith.onclick = () => {
      state.modalContext = { invoiceWithSign: true };
      hideModal();
      printPagesForSelected("both");
    };
    btnWithout.onclick = () => {
      state.modalContext = { invoiceWithSign: false };
      hideModal();
      printPagesForSelected("both");
    };
    showModal("Label & Invoice Options", container);
  });
  updatePrintButtons();

  const ordersFileInput = document.getElementById("orders-file-input");
  const startImportBtn = document.getElementById("btn-start-import");
  if (ordersFileInput && startImportBtn) {
    ordersFileInput.addEventListener("change", () => {
      startImportBtn.disabled = !ordersFileInput.files.length;
    });
    startImportBtn.addEventListener("click", async () => {
      const file = ordersFileInput.files[0];
      if (!file) return;
      const parsed = await parseFile(file);
      state.importPreview = parsed.rows;
      state.importHeaders = parsed.headers;
      state.importMapping = {};
      const body = buildMappingUI(parsed.headers);
      state.modalContext = "import";
      showModal("Map Columns", body);
    });
  }

  const modalClose = document.getElementById("modal-close");
  if (modalClose) modalClose.addEventListener("click", hideModal);
  const modalEl = $("#modal");
  modalEl.addEventListener("click", e => {
    if (e.target === modalEl) {
      if (state.preventModalClose) return;
      hideModal();
    }
  });
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") {
      if (state.preventModalClose) return;
      hideModal();
    }
  });
  const modalConfirm = document.getElementById("modal-confirm");
  if (modalConfirm) {
    modalConfirm.addEventListener("click", () => {
      if (state.modalContext !== "import") return;
      const warnBody = buildImportWarningUI();
      showModal("Review Import", warnBody);
      const s = $("#import-summary");
      if (s) s.textContent = "Ready to import " + ((state.importPreview && state.importPreview.length) || 0) + " rows based on current mapping.";
    });
  }
  const modalPrev = document.getElementById("modal-prev");
  if (modalPrev) {
    modalPrev.addEventListener("click", () => {
      if (state.modalContext !== "import") return;
      const body = buildMappingUI(state.importHeaders || []);
      state.modalContext = "import";
      showModal("Map Columns", body);
    });
  }
  const modalNext = document.getElementById("modal-next");
  if (modalNext) {
    modalNext.addEventListener("click", () => {
      if (state.modalContext !== "import") return;
      const res = applyImport(state.importMapping, state.importPreview || []);
      const s = $("#import-summary");
      if (s) s.textContent = "Imported " + res.added + " new orders, skipped " + res.skipped + " duplicates.";
      renderOrders();
      renderPendingOrders();
      setTimeout(hideModal, 1200);
    });
  }

  const skuFileInput = document.getElementById("sku-hsn-file-input");
  const skuBtn = document.getElementById("btn-import-sku-hsn");
  if (skuFileInput && skuBtn) {
    skuFileInput.addEventListener("change", () => {
      skuBtn.disabled = !skuFileInput.files.length;
    });
    skuBtn.addEventListener("click", async () => {
      const file = skuFileInput.files[0];
      const parsed = await parseFile(file);
      state.skuHsn = appendUniqueRows(state.skuHsn, parsed.rows, row => String(row["ManufacturerModelNo"] || row["SupplierCode"] || ""));
      await saveState();
      renderSkuHsn();
    });
  }

  const hsnFileInput = document.getElementById("hsn-percent-file-input");
  const hsnBtn = document.getElementById("btn-import-hsn-percent");
  if (hsnFileInput && hsnBtn) {
    hsnFileInput.addEventListener("change", () => {
      hsnBtn.disabled = !hsnFileInput.files.length;
    });
    hsnBtn.addEventListener("click", async () => {
      const file = hsnFileInput.files[0];
      const parsed = await parseFile(file);
      state.hsnPercent = appendUniqueRows(state.hsnPercent, parsed.rows, row => String(row["HSN CODE"] || ""));
      await saveState();
      renderHsnPercent();
    });
  }

  // Spillbox removed from UI

  // update via row click only

  switchView("orders");
  hideModal();
}

function confirmDelete(rows) {
  if (!rows || !rows.length) return;
  const body = document.createElement("div");
  const warn = document.createElement("div");
  warn.className = "warning";
  const ids = rows.map(r => r.uniqueId || "(no UniqueId)").join(", ");
  const count = rows.length;
  warn.textContent = `Unique ID(s): "${ids}" will be deleted permanently. Number of orders selected to delete: ${count}.`;
  body.appendChild(warn);
  const footer = document.querySelector(".modal-footer");
  const prev = document.getElementById("modal-prev");
  const next = document.getElementById("modal-next");
  const confirmBtn = document.getElementById("modal-confirm");
  if (prev) prev.classList.add("hidden");
  if (next) next.classList.add("hidden");
  if (confirmBtn) {
    confirmBtn.classList.remove("hidden");
    confirmBtn.textContent = "Delete";
    confirmBtn.onclick = async () => {
      showLoading();
      const setDel = new Set(rows);
      state.orders = state.orders.filter(r => !setDel.has(r));
      await saveState();
      renderOrders();
      renderPendingOrders();
      hideModal();
      setTimeout(hideLoading, 200);
    };
  }
  showModal("Confirm Delete", body);
}

function showLoading() { document.getElementById("loading-overlay").classList.remove("hidden"); }
function hideLoading() { document.getElementById("loading-overlay").classList.add("hidden"); }

function escapeCsv(v) {
  const s = String(v ?? "");
  if (s.includes(",") || s.includes("\"") || s.includes("\n")) {
    return "\"" + s.replace(/"/g, "\"\"") + "\"";
  }
  return s;
}

function appendUniqueRows(existing, incoming, keyFn) {
  const keys = new Set(existing.map(r => keyFn(r)));
  const out = existing.slice();
  for (const r of incoming) {
    const k = keyFn(r);
    if (!k) continue;
    if (!keys.has(k)) {
      out.push(r);
      keys.add(k);
    }
  }
  return out;
}

function buildUpdateDialog(order) {
  const container = document.createElement("div");
  state.preventModalClose = true;
  state.modalContext = "update";
  const tabs = document.createElement("div");
  tabs.className = "tabs";
  const tabManual = document.createElement("button");
  tabManual.className = "tab-btn active";
  tabManual.textContent = "Manual Fill";
  const tabAuto = document.createElement("button");
  tabAuto.className = "tab-btn";
  tabAuto.textContent = "Auto Fetch";
  tabs.appendChild(tabManual);
  tabs.appendChild(tabAuto);
  container.appendChild(tabs);
  const form = document.createElement("div");
  form.className = "form-grid";
  const fields = [
    ["courierName","CourierName"],
    ["awb","AWB"],
    ["weightKg","Weight (in Kg)"],
    ["transportMode","Mode of Transportation"],
    ["lbh","LBH"]
  ];
  const inputs = {};
  const errors = {};
  fields.forEach(([key,label]) => {
    const f = document.createElement("div");
    f.className = "form-field";
    const lab = document.createElement("label");
    lab.textContent = label;
    const inp = document.createElement("input");
    inp.value = "";
    inputs[key] = inp;
    const err = document.createElement("div");
    err.className = "error-text";
    err.textContent = "";
    errors[key] = err;
    f.appendChild(lab);
    f.appendChild(inp);
    f.appendChild(err);
    form.appendChild(f);
  });
  const dateFields = [
    ["dispatchDate","DispatchDate"],
    ["invoiceDate","InvoiceDate"]
  ];
  function formatMMDDYYYY(d) {
    const mm = String(d.getMonth() + 1).padStart(2,"0");
    const dd = String(d.getDate()).padStart(2,"0");
    const yyyy = d.getFullYear();
    return mm + "/" + dd + "/" + yyyy;
  }
  function isoFromMMDDYYYY(s) {
    const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!m) return null;
    const mm = parseInt(m[1],10);
    const dd = parseInt(m[2],10);
    const yyyy = parseInt(m[3],10);
    const dt = new Date(yyyy, mm - 1, dd);
    if (dt.getMonth() + 1 !== mm || dt.getDate() !== dd || dt.getFullYear() !== yyyy) return null;
    return dt.toISOString().slice(0,10);
  }
  dateFields.forEach(([key,label]) => {
    const f = document.createElement("div");
    f.className = "form-field";
    const labWrap = document.createElement("div");
    labWrap.style.display = "flex";
    labWrap.style.alignItems = "center";
    labWrap.style.gap = "8px";
    const lab = document.createElement("label");
    lab.textContent = label;
    const fmt = document.createElement("span");
    fmt.className = "error-text";
    fmt.textContent = "(Format: mm/dd/yyyy)";
    labWrap.appendChild(lab);
    labWrap.appendChild(fmt);
    const disp = document.createElement("input");
    disp.setAttribute("type","text");
    disp.placeholder = "mm/dd/yyyy";
    const pick = document.createElement("input");
    pick.setAttribute("type","date");
    pick.style.display = "none";
    const icon = document.createElement("button");
    icon.textContent = "📅";
    icon.className = "icon-btn";
    icon.style.color = "#ffffff";
    icon.addEventListener("click", () => {
      if (pick.showPicker) pick.showPicker();
      else pick.click();
    });
    const today = new Date();
    disp.value = formatMMDDYYYY(today);
    pick.value = today.toISOString().slice(0,10);
    inputs[key] = disp;
    inputs[key+"Picker"] = pick;
    const err = document.createElement("div");
    err.className = "error-text";
    err.textContent = "";
    errors[key] = err;
    disp.addEventListener("input", () => {
      const iso = isoFromMMDDYYYY(disp.value.trim());
      if (iso) {
        pick.value = iso;
        err.textContent = "";
      }
    });
    pick.addEventListener("change", () => {
      const d = pick.value ? new Date(pick.value) : null;
      if (d) {
        disp.value = formatMMDDYYYY(d);
        err.textContent = "";
      }
    });
    f.appendChild(labWrap);
    const dateRow = document.createElement("div");
    dateRow.style.display = "flex";
    dateRow.style.gap = "8px";
    dateRow.appendChild(disp);
    dateRow.appendChild(icon);
    f.appendChild(dateRow);
    f.appendChild(pick);
    f.appendChild(err);
    form.appendChild(f);
  });
  container.appendChild(form);
  const autoArea = document.createElement("div");
  autoArea.className = "hidden";
  const autoFields = [
    ["poValue","PoValue"],["totalQuantity","TotalQuantity"],["totalPoValue","TotalPoValue"],["biwpo","BIWPO"],["companyGstin","Company GSTIN"],["hsnCode","HSN Code"],["invoiceNumber","InvoiceNumber"],["unitPriceNoTax","UnitPrice-NoTax"],["netAmount","NetAmount"],["taxRate","TaxRate"],["taxType","TaxType"],["taxPercent","Tax Percent"],["taxAmount","TaxAmount"],["totalAmount","TotalAmount"],["mrp","Mrp"]
  ];
  const autoGrid = document.createElement("div");
  autoGrid.className = "form-grid";
  const previewInputs = {};
  autoFields.forEach(([key,label]) => {
    const f = document.createElement("div");
    f.className = "form-field";
    const lab = document.createElement("label");
    lab.textContent = label;
    const inp = document.createElement("input");
    inp.readOnly = true;
    previewInputs[key] = inp;
    f.appendChild(lab);
    f.appendChild(inp);
    autoGrid.appendChild(f);
  });
  autoArea.appendChild(autoGrid);
  const autoBtn = document.createElement("button");
  autoBtn.textContent = "Fetch and Apply";
  autoBtn.addEventListener("click", () => {
    const tmp = JSON.parse(JSON.stringify(order));
    computeAutoFetch(tmp, { assignInvoice: false });
    autoFields.forEach(([key]) => {
      if (previewInputs[key]) previewInputs[key].value = tmp[key] ?? "";
    });
  });
  const gstField = document.createElement("div");
  gstField.className = "form-field";
  const gstLab = document.createElement("label");
  gstLab.textContent = "Company GSTIN (editable)";
  const gstInp = document.createElement("input");
  gstInp.value = state.companyGstinDefault;
  gstInp.addEventListener("input", () => {
    state.companyGstinDefault = gstInp.value;
    saveState();
  });
  gstField.appendChild(gstLab);
  gstField.appendChild(gstInp);
  autoArea.appendChild(gstField);
  autoArea.appendChild(autoBtn);
  container.appendChild(autoArea);
  const prevBtn = document.getElementById("modal-prev");
  const nextBtn = document.getElementById("modal-next");
  const confirmBtn = document.getElementById("modal-confirm");
  function validateManual() {
    const isAlpha = s => /^[A-Za-z ]+$/.test(s);
    const isAlphaNum = s => /^[A-Za-z0-9]+$/.test(s);
    const isNumeric = s => /^-?\d+(\.\d+)?$/.test(s);
    const isLbh = s => /[0-9].*\*/.test(s);
    inputs.courierName.setAttribute("type", "text");
    inputs.awb.setAttribute("type", "text");
    inputs.weightKg.setAttribute("type", "number");
    inputs.weightKg.setAttribute("step", "0.01");
    inputs.transportMode.setAttribute("type", "text");
    inputs.lbh.setAttribute("type", "text");
    const v = k => (inputs[k]?.value || "").trim();
    Object.keys(errors).forEach(k => { errors[k].textContent = ""; });
    let ok = true;
    if (!v("courierName")) { errors.courierName.textContent = "Required"; ok = false; }
    else if (!isAlpha(v("courierName"))) { errors.courierName.textContent = "Only letters and spaces allowed"; ok = false; }
    if (!v("transportMode")) { errors.transportMode.textContent = "Required"; ok = false; }
    else if (!isAlpha(v("transportMode"))) { errors.transportMode.textContent = "Only letters and spaces allowed"; ok = false; }
    if (!v("awb")) { errors.awb.textContent = "Required"; ok = false; }
    else if (!isAlphaNum(v("awb"))) { errors.awb.textContent = "Alphanumeric only"; ok = false; }
    if (!v("weightKg")) { errors.weightKg.textContent = "Required"; ok = false; }
    else if (!isNumeric(v("weightKg"))) { errors.weightKg.textContent = "Must be a number"; ok = false; }
    if (!v("lbh")) { errors.lbh.textContent = "Required"; ok = false; }
    else if (!isLbh(v("lbh"))) { errors.lbh.textContent = "Must contain numbers and '*' (e.g., 10*20*30)"; ok = false; }
    const dateFmt = /^(\d{2})\/(\d{2})\/(\d{4})$/;
    if (!inputs.dispatchDate.value) { errors.dispatchDate.textContent = "Required"; ok = false; }
    else if (!dateFmt.test(inputs.dispatchDate.value)) { errors.dispatchDate.textContent = "Invalid format mm/dd/yyyy"; ok = false; }
    if (!inputs.invoiceDate.value) { errors.invoiceDate.textContent = "Required"; ok = false; }
    else if (!dateFmt.test(inputs.invoiceDate.value)) { errors.invoiceDate.textContent = "Invalid format mm/dd/yyyy"; ok = false; }
    return ok;
  }
  function showManualFooter() {
    prevBtn.classList.add("hidden");
    confirmBtn.classList.add("hidden");
    nextBtn.classList.remove("hidden");
    nextBtn.textContent = "Next";
    nextBtn.onclick = () => {
      if (!validateManual()) return;
      tabAuto.click();
    };
  }
  function showAutoFooter() {
    nextBtn.classList.add("hidden");
    prevBtn.classList.add("hidden");
    confirmBtn.classList.remove("hidden");
    confirmBtn.textContent = "Save";
    confirmBtn.onclick = () => {
      const tmp = JSON.parse(JSON.stringify(order));
      Object.entries(inputs).forEach(([k,inp]) => {
        if (k.endsWith("Picker")) return;
        tmp[k] = inp.value;
      });
      const dIso = isoFromMMDDYYYY(inputs.dispatchDate.value);
      const iIso = isoFromMMDDYYYY(inputs.invoiceDate.value);
      tmp.dispatchDate = inputs.dispatchDate.value;
      tmp.invoiceDate = inputs.invoiceDate.value;
      tmp.dispatchDateIso = dIso || null;
      tmp.invoiceDateIso = iIso || null;
      computeAutoFetch(tmp, { assignInvoice: false });
      const missing = [];
      autoFields.forEach(([key,label]) => {
        if (key === "invoiceNumber") return;
        const val = tmp[key];
        if (val === undefined || val === null || String(val).trim() === "") {
          missing.push(label);
        }
      });
      if (missing.length) {
        alert("Auto Fetch missing fields: " + missing.join(", "));
        return;
      }
      Object.assign(order, tmp);
      computeAutoFetch(order);
      order.orderStatus = "Shipped";
      order.lastUpdated = new Date().toLocaleString();
      if (!order.invoiceNumber) order.invoiceNumber = nextInvoiceNumber();
      order.vendorInvoiceNumber = order.invoiceNumber;
      Promise.resolve(saveState()).then(() => {
        state.orders.forEach(r => { r.__selectedPending = false; r.__selected = false; });
        renderOrders();
        renderPendingOrders();
        hideModal();
        updateUpdateSelectedButton();
      });
    };
  }
  tabManual.addEventListener("click", () => {
    tabManual.classList.add("active");
    tabAuto.classList.remove("active");
    form.classList.remove("hidden");
    autoArea.classList.add("hidden");
    showManualFooter();
  });
  tabAuto.addEventListener("click", () => {
    if (!validateManual()) return;
    tabAuto.classList.add("active");
    tabManual.classList.remove("active");
    form.classList.add("hidden");
    autoArea.classList.remove("hidden");
    const tmp = JSON.parse(JSON.stringify(order));
    computeAutoFetch(tmp, { assignInvoice: false });
    autoFields.forEach(([key]) => {
      if (previewInputs[key]) previewInputs[key].value = tmp[key] ?? "";
    });
    showAutoFooter();
  });
  showModal("Update Order", container);
  showManualFooter();
}

function renderDashboard() {
  const startEl = document.getElementById("dash-start-date");
  const endEl = document.getElementById("dash-end-date");
  const applyBtn = document.getElementById("dash-apply");
  const resetBtn = document.getElementById("dash-reset");
  const totalEl = document.getElementById("dash-total");
  const shippedEl = document.getElementById("dash-shipped");
  const pendingEl = document.getElementById("dash-pending");
  const topSkusEl = document.getElementById("dash-top-skus");
  const monthlyEl = document.getElementById("dash-monthly-orders");
  function withinRange(o) {
    if (!state.dashboardFilter) return true;
    const d = orderDate(o);
    if (!d) return false;
    const t = new Date(d).getTime();
    return t >= state.dashboardFilter.start && t <= state.dashboardFilter.end;
  }
  function orderDate(o) {
    const a = o.invoiceDateIso || o.dispatchDateIso || o.invoiceDate || o.dispatchDate || o.poDate;
    if (!a || !String(a).trim()) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(a)) return a;
    const m = String(a).match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (m) {
      const mm = parseInt(m[1],10);
      const dd = parseInt(m[2],10);
      const yyyy = parseInt(m[3],10);
      const dt = new Date(yyyy, mm - 1, dd);
      return dt.toISOString().slice(0,10);
    }
    return a;
  }
  function updateCounts() {
    const orders = state.orders.filter(withinRange);
    totalEl.textContent = String(orders.length);
    shippedEl.textContent = String(orders.filter(o => String(o.orderStatus).toLowerCase() === "shipped").length);
    pendingEl.textContent = String(orders.filter(o => String(o.orderStatus).toLowerCase() === "pending").length);
    if (topSkusEl) {
      renderTopSkus(orders);
    }
    if (monthlyEl) {
      renderMonthlyOrders(orders);
    }
  }
  function renderTopSkus(orders) {
    const shipped = orders.filter(o => String(o.orderStatus).toLowerCase() === "shipped");
    const qtyBySku = new Map();
    shipped.forEach(o => {
      const sku = String(o.productCode || "").trim() || "(unknown)";
      const q = parseFloat(o.totalQuantity || o.quantity || 0) || 0;
      qtyBySku.set(sku, (qtyBySku.get(sku) || 0) + q);
    });
    const sorted = Array.from(qtyBySku.entries()).sort((a,b) => b[1] - a[1]).slice(0, 10);
    let start = state.dashboardFilter ? state.dashboardFilter.start : null;
    let end = state.dashboardFilter ? state.dashboardFilter.end : null;
    if (!start || !end) {
      const times = shipped.map(o => {
        const d = orderDate(o);
        return d ? new Date(d).getTime() : null;
      }).filter(Boolean).sort((a,b) => a - b);
      if (times.length) {
        start = times[0];
        end = times[times.length - 1];
      } else {
        const now = Date.now();
        start = now - 30*24*60*60*1000;
        end = now;
      }
    }
    const buckets = 30;
    const span = Math.max(1, end - start);
    const seriesBySku = new Map();
    sorted.forEach(([sku]) => {
      const series = new Array(buckets).fill(0);
      shipped.forEach(o => {
        const s = String(o.productCode || "").trim() || "(unknown)";
        if (s !== sku) return;
        const d = orderDate(o);
        if (!d) return;
        const t = new Date(d).getTime();
        if (t < start || t > end) return;
        const idx = Math.min(buckets - 1, Math.max(0, Math.floor(((t - start) / span) * (buckets - 1))));
        const q = parseFloat(o.totalQuantity || o.quantity || 0) || 0;
        series[idx] += q;
      });
      seriesBySku.set(sku, series);
    });
    function resolveProductName(code) {
      const key = String(code || "").trim().toLowerCase();
      const match = state.skuHsn.find(r => String(r.productKey || "").toLowerCase() === key);
      const name = match ? (match["ProductName"] || match["DescriptionofProduct"] || "") : "";
      return name;
    }
    topSkusEl.innerHTML = "";
    topSkusEl.classList.add("sku-grid");
    sorted.forEach(([sku]) => {
      const series = seriesBySku.get(sku) || [];
      const maxY = Math.max(1, ...series);
      const w = 280;
      const h = 64;
      const stepX = w / (series.length - 1 || 1);
      let d = "";
      series.forEach((y, i) => {
        const x = Math.round(i * stepX);
        const py = Math.round(h - (y / maxY) * h);
        d += (i === 0 ? "M " : " L ") + x + " " + py;
      });
      const card = document.createElement("div");
      card.className = "sku-card";
      const title = document.createElement("div");
      title.className = "sku-title";
      title.textContent = sku;
      const name = document.createElement("div");
      name.className = "sku-name";
      name.textContent = resolveProductName(sku) || "";
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      svg.setAttribute("class", "sparkline");
      svg.setAttribute("viewBox", "0 0 " + w + " " + h);
      svg.setAttribute("preserveAspectRatio","none");
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", d.trim());
      path.setAttribute("fill", "none");
      path.setAttribute("stroke", getComputedStyle(document.documentElement).getPropertyValue("--accent-2").trim() || "#38bdf8");
      path.setAttribute("stroke-width", "2");
      svg.appendChild(path);
      card.appendChild(title);
      card.appendChild(name);
      card.appendChild(svg);
      topSkusEl.appendChild(card);
    });
  }
  function monthKey(d) {
    const dt = new Date(d);
    const m = dt.getMonth();
    const y = dt.getFullYear();
    return y + "-" + String(m+1).padStart(2,"0");
  }
  function monthLabel(key) {
    const [y,m] = key.split("-");
    const dt = new Date(parseInt(y,10), parseInt(m,10)-1, 1);
    return dt.toLocaleString("en-US", { month: "short" }) + "'" + String(dt.getFullYear()).slice(2);
  }
  function renderMonthlyOrders(orders) {
    const s = state.dashboardFilter ? state.dashboardFilter.start : null;
    const e = state.dashboardFilter ? state.dashboardFilter.end : null;
    const times = orders.map(o => {
      const d = orderDate(o);
      return d ? new Date(d).getTime() : null;
    }).filter(Boolean).sort((a,b) => a - b);
    let start = s || (times[0] || Date.now() - 180*24*60*60*1000);
    let end = e || (times[times.length - 1] || Date.now());
    const startDt = new Date(start);
    startDt.setDate(1);
    const endDt = new Date(end);
    endDt.setDate(1);
    const months = [];
    const cur = new Date(startDt);
    while (cur <= endDt) {
      months.push(cur.getFullYear() + "-" + String(cur.getMonth()+1).padStart(2,"0"));
      cur.setMonth(cur.getMonth()+1);
    }
    const shippedSeries = months.map(() => 0);
    const pendingSeries = months.map(() => 0);
    orders.forEach(o => {
      const d = orderDate(o);
      if (!d) return;
      const t = new Date(d).getTime();
      if (t < start || t > end) return;
      const key = monthKey(d);
      const idx = months.indexOf(key);
      if (idx === -1) return;
      if (String(o.orderStatus).toLowerCase() === "shipped") shippedSeries[idx] += 1;
      else if (String(o.orderStatus).toLowerCase() === "pending") pendingSeries[idx] += 1;
    });
    monthlyEl.innerHTML = "";
    const w = 800;
    const h = 140;
    const maxY = Math.max(1, ...shippedSeries, ...pendingSeries);
    const stepX = w / (months.length - 1 || 1);
    function seriesPath(series) {
      let d = "";
      series.forEach((y, i) => {
        const x = Math.round(i * stepX);
        const py = Math.round(h - (y / maxY) * h);
        d += (i === 0 ? "M " : " L ") + x + " " + py;
      });
      return d.trim();
    }
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "sparkline");
    svg.setAttribute("viewBox", "0 0 " + w + " " + h);
    svg.setAttribute("preserveAspectRatio","none");
    const shippedPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    shippedPath.setAttribute("d", seriesPath(shippedSeries));
    shippedPath.setAttribute("fill", "none");
    shippedPath.setAttribute("stroke", getComputedStyle(document.documentElement).getPropertyValue("--accent-2").trim() || "#38bdf8");
    shippedPath.setAttribute("stroke-width", "2");
    const pendingPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    pendingPath.setAttribute("d", seriesPath(pendingSeries));
    pendingPath.setAttribute("fill", "none");
    pendingPath.setAttribute("stroke", getComputedStyle(document.documentElement).getPropertyValue("--accent").trim() || "#22c55e");
    pendingPath.setAttribute("stroke-width", "2");
    svg.appendChild(shippedPath);
    svg.appendChild(pendingPath);
    monthlyEl.appendChild(svg);
    const labels = document.createElement("div");
    labels.className = "axis-labels";
    labels.style.gridTemplateColumns = "repeat(" + months.length + ", 1fr)";
    const maxLabels = Math.min(12, months.length);
    const step = Math.max(1, Math.floor(months.length / maxLabels));
    for (let i = 0; i < months.length; i++) {
      const span = document.createElement("div");
      span.textContent = (i % step === 0) ? monthLabel(months[i]) : "";
      labels.appendChild(span);
    }
    monthlyEl.appendChild(labels);
  }
  applyBtn.onclick = () => {
    const s = startEl.value ? new Date(startEl.value).setHours(0,0,0,0) : null;
    const e = endEl.value ? new Date(endEl.value).setHours(23,59,59,999) : null;
    if (!s || !e) {
      alert("Please select both Start and End date, or click Set All.");
      return;
    }
    state.dashboardFilter = { start: s, end: e };
    updateCounts();
  };
  resetBtn.onclick = () => {
    startEl.value = "";
    endEl.value = "";
    state.dashboardFilter = null;
    updateCounts();
  };
  updateCounts();
}

document.addEventListener("DOMContentLoaded", () => { onReady(); });
