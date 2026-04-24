import axios from "axios";

const api = axios.create({
  baseURL: "http://localhost:8000",
  timeout: 60000,
  headers: {
    "Content-Type": "application/json",
  },
});

// ==========================
// LIVE APIs
// ==========================
export const getLiveRisk = async () => (await api.get("/live/risk")).data;
export const getLiveData = async () => (await api.get("/live/")).data;
export const getLiveClimate = async () => (await api.get("/live/climate")).data;
export const getLiveEconomy = async () => (await api.get("/live/economy")).data;
export const getLiveTrade = async () => (await api.get("/live/trade")).data;
export const getLiveGeopolitics = async () => (await api.get("/live/geopolitics")).data;
export const getLiveMigration = async () => (await api.get("/live/migration")).data;
export const getLiveSocial = async () => (await api.get("/live/social")).data;
export const getLiveInfrastructure = async () => (await api.get("/live/infrastructure")).data;

// ==========================
// INTERCONNECTION (GLOBAL)
// ==========================
export const getHistoricalRisk = async (year) =>
  (await api.get(`/interconnection/history/${year}`)).data;

export const getGlobalTrend = async (country = "IND") =>
  (await api.get(`/interconnection/trend/${country}`)).data;

export const getLatestGlobal = async (country = "IND") =>
  (await api.get(`/interconnection/latest/${country}`)).data;

export const getHighRiskMonths = async (country = "IND") =>
  (await api.get(`/interconnection/high-risk/${country}`)).data;

export const getGlobalSummary = async (country = "IND") =>
  (await api.get(`/interconnection/summary/${country}`)).data;

export const getStateRisk = async (state) =>
  (await api.get(`/interconnection/state/${state}`)).data;

export const getStateImpact = async (state) =>
  (await api.get(`/interconnection/state-impact/${state}`)).data;

// ==========================
// ECONOMY
// ==========================
export const getEconomyTrend = async (country = "IND") =>
  (await api.get(`/economy/trend/${country}`)).data;

export const getEconomyLatest = async (country = "IND") =>
  (await api.get(`/economy/latest/${country}`)).data;

export const getEconomySummary = async (country = "IND") =>
  (await api.get(`/economy/summary/${country}`)).data;

export const getEconomyCountries = async () =>
  (await api.get(`/economy/countries`)).data;

// ==========================
// SOCIAL
// ==========================
export const getSocialTrend = async (state = "Karnataka") =>
  (await api.get(`/social/trend/${state}`)).data;

export const getSocialLatest = async (state = "Karnataka") =>
  (await api.get(`/social/latest/${state}`)).data;

export const getSocialSummary = async (state = "Karnataka") =>
  (await api.get(`/social/summary/${state}`)).data;

export const getSocialStates = async () =>
  (await api.get(`/social/states`)).data;

// ==========================
// MIGRATION
// ==========================
export const getMigrationTrend = async (country = "IND") =>
  (await api.get(`/migration/trend/${country}`)).data;

export const getMigrationLatest = async (country = "IND") =>
  (await api.get(`/migration/latest/${country}`)).data;

export const getMigrationSummary = async (country = "IND") =>
  (await api.get(`/migration/summary/${country}`)).data;

// ==========================
// TRADE
// ==========================
export const getTradeCountry = async (country = "IND") =>
  (await api.get(`/trade-risk/country/${country}`)).data;

export const getTradeTop = async () =>
  (await api.get(`/trade-risk/top`)).data;

export const getTradeSummary = async () =>
  (await api.get(`/trade-risk/summary`)).data;

export const getTradeAll = async () =>
  (await api.get(`/trade-risk/all`)).data;

// ==========================
// CLIMATE
// ==========================
export const getClimateState = async (state) =>
  (await api.get(`/climate-risk/state/${state}`)).data;

export const getClimateDistrict = async (district) =>
  (await api.get(`/climate-risk/district/${district}`)).data;

export const getClimateTopStates = async () =>
    (await api.get(`/climate-risk/top-states`)).data;

export const getClimateTopDistricts = async () =>
  (await api.get(`/climate-risk/top-districts`)).data;

export const getClimateAllStates = async () =>
  (await api.get(`/climate-risk/states`)).data;

export const getClimateAllDistricts = async () =>
  (await api.get(`/climate-risk/districts`)).data;

export const getInterconnectionDistrict = async (district) =>
  (await api.get(`/climate-risk/interconnection/district/${district}`)).data;

export const getInterconnectionTopDistricts = async () =>
  (await api.get(`/climate-risk/interconnection/top-districts`)).data;

export const getInterconnectionSummary = async () =>
  (await api.get(`/climate-risk/interconnection/summary`)).data;

// ==========================
// GEOPOLITICS
// ==========================
export const getGeopoliticsCountry = async (country = "IND") =>
  (await api.get(`/geopolitics-risk/country/${country}`)).data;

export const getGeopoliticsTop = async () =>
  (await api.get(`/geopolitics-risk/top-countries`)).data;

export const getGeopoliticsGlobalSummary = async () =>
  (await api.get(`/geopolitics-risk/global-summary`)).data;

export const getGeopoliticsCountries = async () => {
    const res = await api.get(`/geopolitics-risk/top-countries?top_n=250`);
    return res.data.countries.map(c => c.country).sort();
};

// ==========================
// INFRASTRUCTURE
// ==========================
export const getInfraStateYear = async (state, year) =>
  (await api.get(`/infrastructure-risk/state/${state}/${year}`)).data;

export const getInfraLatest = async (state) =>
  (await api.get(`/infrastructure-risk/latest/${state}`)).data;

export const getInfraTrend = async (state) =>
  (await api.get(`/infrastructure-risk/trend/${state}`)).data;

export const getInfraSummary = async () =>
  (await api.get(`/infrastructure-risk/summary`)).data;

export const getInfraTopStates = async () =>
  (await api.get(`/infrastructure-risk/top-states`)).data;

export const getInfraAvailableStates = async () =>
  (await api.get(`/infrastructure-risk/states`)).data;

// ==========================
// SIMULATION / OTHER
// ==========================
export const runWhatIfSimulation = async (payload) => {
  const res = await api.post("/interconnection/what-if", payload);
  return res.data;
};

export const simulateShock = async (sector, value) => {
  const res = await api.get(`/interconnection/shock/${sector}/${value}`);
  return res.data;
};

// ==========================
// UNIFIED HISTORICAL API
// ==========================
export const getHistoricalSector = (sector, params) =>
  api.get(`/historical/${sector}`, { params });

export default api;

