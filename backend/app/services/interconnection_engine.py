"""
🌍 Global Risk Interconnection Engine

This module calculates interconnected risk scores across multiple sectors
by combining climate, trade, and geopolitics risks into unified country-level scores.

Sectors:
- Climate Risk (district level → aggregated to country)
- Trade Risk (country level)
- Geopolitics Risk (country level)
- Combined Interconnected Risk
"""

import pandas as pd
import numpy as np
import os

# ================================================================
# 📂 LOAD DATA
# ================================================================

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))

# Climate data (district level)
climate_path = os.path.join(BASE_PATH, "data", "processed", "climate", "climate_risk_district.csv")
climate_df = pd.read_csv(climate_path)

print("✅ Climate data loaded for interconnection analysis!")
print(f"   Districts: {len(climate_df)}")
print(f"   Mean Climate Risk: {climate_df['predicted_risk'].mean():.4f}")

# Trade data (country level)
trade_path = os.path.join(BASE_PATH, "data", "processed", "trade", "trade_risk_country.csv")
trade_df = pd.read_csv(trade_path)

print("\n✅ Trade data loaded for interconnection analysis!")
print(f"   Countries: {len(trade_df)}")
print(f"   Mean Trade Risk: {trade_df['Trade_Risk'].mean():.4f}")

# Geopolitics data (country level)
geopolitics_path = os.path.join(BASE_PATH, "data", "processed", "geopolitics", "geopolitics_risk_output_country.csv")
geopolitics_df = pd.read_csv(geopolitics_path)

print("\n✅ Geopolitics data loaded for interconnection analysis!")
print(f"   Countries: {len(geopolitics_df)}")
print(f"   Mean Geopolitics Risk: {geopolitics_df['latest_risk'].mean():.4f}")

# ================================================================
# 🔧 AGGREGATE CLIMATE TO COUNTRY LEVEL
# ================================================================

def aggregate_climate_to_country(climate_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate district-level climate risk to country/state level.
    
    Args:
        climate_df (pd.DataFrame): District-level climate risk data
        
    Returns:
        pd.DataFrame: Country/State-level aggregated climate risk
    """
    print("\n🔧 Aggregating climate risk to country/state level...")
    
    # Group by State and calculate mean predicted_risk
    climate_country = climate_df.groupby('State')['predicted_risk'].mean().reset_index()
    climate_country.columns = ['Country', 'climate_risk']
    
    print(f"   ✅ Aggregated {len(climate_df)} districts to {len(climate_country)} regions")
    print(f"   ✅ Mean climate risk: {climate_country['climate_risk'].mean():.4f}")
    
    return climate_country


# ================================================================
# 🔧 MERGE DATASETS
# ================================================================

def merge_risk_datasets(climate_country: pd.DataFrame, trade_df: pd.DataFrame, geopolitics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge climate, trade, and geopolitics risk datasets on country level.
    
    This function performs sequential merges to combine all three risk sources.
    Handles special case where climate data contains Indian states.
    Uses geopolitics as base (ISO-3 standard) and maps trade/climate to it.
    
    Args:
        climate_country (pd.DataFrame): Country/State-level climate risk
        trade_df (pd.DataFrame): Country-level trade risk
        geopolitics_df (pd.DataFrame): Country-level geopolitics risk (ISO-3)
        
    Returns:
        pd.DataFrame: Merged risk dataset with all three sectors
    """
    print("\n🔧 Merging climate, trade, and geopolitics risk datasets...")
    print("   Strategy: Use geopolitics (ISO-3) as base, map others to it")
    
    # ================================================================
    # STEP 1: PREPARE GEOPOLITICS DATA (BASE DATASET - ISO-3)
    # ================================================================
    
    # Get unique countries from geopolitics (already ISO-3)
    geopolitics_clean = geopolitics_df[['Country', 'latest_risk']].copy()
    geopolitics_clean.columns = ['Country', 'geopolitics_risk']
    
    print(f"\n   📊 Geopolitics base: {len(geopolitics_clean)} countries (ISO-3 codes)")
    
    # ================================================================
    # STEP 2: MAP TRADE DATA TO ISO-3 COUNTRY CODES
    # ================================================================
    
    print("\n   🔧 Mapping trade data to ISO-3 codes...")
    
    # Create country name to ISO-3 mapping (common mappings)
    country_to_iso = {
        # Major countries
        'United States of America': 'USA',
        'United States': 'USA',
        'China': 'CHN',
        'India': 'IND',
        'Germany': 'DEU',
        'France': 'FRA',
        'United Kingdom': 'GBR',
        'Japan': 'JPN',
        'Australia': 'AUS',
        'Canada': 'CAN',
        'Brazil': 'BRA',
        'Russia': 'RUS',
        'South Korea': 'KOR',
        'Italy': 'ITA',
        'Spain': 'ESP',
        'Mexico': 'MEX',
        'Indonesia': 'IDN',
        'Saudi Arabia': 'SAU',
        'Turkey': 'TUR',
        'Switzerland': 'CHE',
        'Poland': 'POL',
        'Belgium': 'BEL',
        'Sweden': 'SWE',
        'Argentina': 'ARG',
        'Norway': 'NOR',
        'Austria': 'AUT',
        'Netherlands': 'NLD',
        'Nigeria': 'NGA',
        'Israel': 'ISR',
        'South Africa': 'ZAF',
        'Denmark': 'DNK',
        'Singapore': 'SGP',
        'Malaysia': 'MYS',
        'Philippines': 'PHL',
        'Pakistan': 'PAK',
        'Bangladesh': 'BGD',
        'Vietnam': 'VNM',
        'Thailand': 'THA',
        'Egypt': 'EGY',
        'Chile': 'CHL',
        'Finland': 'FIN',
        'Portugal': 'PRT',
        'Greece': 'GRC',
        'New Zealand': 'NZL',
        'Romania': 'ROU',
        'Czech Republic': 'CZE',
        'Hungary': 'HUN',
        'Ireland': 'IRL',
        'Peru': 'PER',
        'Colombia': 'COL',
        'Morocco': 'MAR',
        'Kenya': 'KEN',
        'Ethiopia': 'ETH',
        'Ghana': 'GHA',
        'Tanzania': 'TZA',
        'Uganda': 'UGA',
        'Zimbabwe': 'ZWE',
        'Angola': 'AGO',
        'Algeria': 'DZA',
        'Iraq': 'IRQ',
        'Iran': 'IRN',
        'Afghanistan': 'AFG',
        'Ukraine': 'UKR',
        'Kazakhstan': 'KAZ',
        'Qatar': 'QAT',
        'Kuwait': 'KWT',
        'UAE': 'ARE',
        'Oman': 'OMN',
        'Venezuela': 'VEN',
        'Ecuador': 'ECU',
        'Guatemala': 'GTM',
        'Dominican Republic': 'DOM',
        'Puerto Rico': 'PRI',
        'Costa Rica': 'CRI',
        'Panama': 'PAN',
        'Uruguay': 'URY',
        'Paraguay': 'PRY',
        'Bolivia': 'BOL',
        'Honduras': 'HND',
        'El Salvador': 'SLV',
        'Nicaragua': 'NIC',
        'Jamaica': 'JAM',
        'Trinidad and Tobago': 'TTO',
        'Bahamas': 'BHS',
        'Barbados': 'BRB',
        'Guyana': 'GUY',
        'Suriname': 'SUR',
        'Fiji': 'FJI',
        'Papua New Guinea': 'PNG',
        'Solomon Islands': 'SLB',
        'Vanuatu': 'VUT',
        'Samoa': 'WSM',
        'Tonga': 'TON',
        'Kiribati': 'KIR',
        'Palau': 'PLW',
        'Micronesia': 'FSM',
        'Marshall Islands': 'MHL',
        'Nauru': 'NRU',
        'Tuvalu': 'TUV',
        'Cook Islands': 'COK',
        'Niue': 'NIU',
        'French Polynesia': 'PYF',
        'New Caledonia': 'NCL',
        'Guam': 'GUM',
        'American Samoa': 'ASM',
        'Northern Mariana Islands': 'MNP',
        'Puerto Rico': 'PRI',
        'Virgin Islands': 'VIR',
        'Greenland': 'GRL',
        'Iceland': 'ISL',
        'Faroe Islands': 'FRO',
        'Gibraltar': 'GIB',
        'Malta': 'MLT',
        'Cyprus': 'CYP',
        'Luxembourg': 'LUX',
        'Monaco': 'MCO',
        'Liechtenstein': 'LIE',
        'San Marino': 'SMR',
        'Vatican City': 'VAT',
        'Andorra': 'AND',
        'Montenegro': 'MNE',
        'Serbia': 'SRB',
        'Croatia': 'HRV',
        'Slovenia': 'SVN',
        'Bosnia and Herzegovina': 'BIH',
        'North Macedonia': 'MKD',
        'Albania': 'ALB',
        'Bulgaria': 'BGR',
        'Slovakia': 'SVK',
        'Lithuania': 'LTU',
        'Latvia': 'LVA',
        'Estonia': 'EST',
        'Belarus': 'BLR',
        'Moldova': 'MDA',
        'Georgia': 'GEO',
        'Armenia': 'ARM',
        'Azerbaijan': 'AZE',
        'Uzbekistan': 'UZB',
        'Turkmenistan': 'TKM',
        'Tajikistan': 'TJK',
        'Kyrgyzstan': 'KGZ',
        'Mongolia': 'MNG',
        'Nepal': 'NPL',
        'Bhutan': 'BTN',
        'Sri Lanka': 'LKA',
        'Maldives': 'MDV',
        'Myanmar': 'MMR',
        'Laos': 'LAO',
        'Cambodia': 'KHM',
        'Brunei': 'BRN',
        'Timor-Leste': 'TLS',
        'Taiwan': 'TWN',
        'Hong Kong': 'HKG',
        'Macau': 'MAC',
        'North Korea': 'PRK',
        'South Korea': 'KOR',
        'Ryukyu Islands': 'JPN',
        'Kuril Islands': 'RUS',
        'Spratly Islands': 'VNM',
        'Paracel Islands': 'CHN',
        'Scarborough Shoal': 'PHL',
        'Senkaku Islands': 'JPN',
        'Tokdo': 'KOR',
        'Kuriles': 'RUS',
        'Southern Kurils': 'RUS',
        'Northern Territories': 'JPN',
        'Abhasia': 'GEO',
        'South Ossetia': 'GEO',
        'Transnistria': 'MDA',
        'Kosovo': 'XKX',
        'Palestine': 'PSE',
        'Western Sahara': 'ESH',
        'Somaliland': 'SOM',
        'Puntland': 'SOM',
        'Catalonia': 'ESP',
        'Basque Country': 'ESP',
        'Galicia': 'ESP',
        'Corsica': 'FRA',
        'Brittany': 'FRA',
        'Occitania': 'FRA',
        'Alsace': 'FRA',
        'Normandy': 'FRA',
        'Aquitaine': 'FRA',
        'Provence': 'FRA',
        'Burgundy': 'FRA',
        'Loire Valley': 'FRA',
        'Champagne': 'FRA',
        'Picardy': 'FRA',
        'Nord-Pas-de-Calais': 'FRA',
        'Lorraine': 'FRA',
        'Franche-Comte': 'FRA',
        'Auvergne': 'FRA',
        'Limousin': 'FRA',
        'Poitou-Charentes': 'FRA',
        'Languedoc-Roussillon': 'FRA',
        'Midi-Pyrenees': 'FRA',
        'Rhone-Alpes': 'FRA',
        'Ile-de-France': 'FRA',
        'Centre-Val de Loire': 'FRA',
        'Bourgogne-Franche-Comte': 'FRA',
        'Normandie': 'FRA',
        'Hauts-de-France': 'FRA',
        'Grand Est': 'FRA',
        'Pays de la Loire': 'FRA',
        'Nouvelle-Aquitaine': 'FRA',
        'Occitanie': 'FRA',
        'Auvergne-Rhone-Alpes': 'FRA',
        'Provence-Alpes-Cote d Azur': 'FRA',
        'British Overseas Territories': 'GBR',
        'French Overseas Territories': 'FRA',
        'Dutch Caribbean': 'NLD',
        'Danish Overseas Territories': 'DNK',
        'Norwegian Overseas Territories': 'NOR',
        'Australian Overseas Territories': 'AUS',
        'New Zealand Overseas Territories': 'NZL',
        'US Overseas Territories': 'USA',
        'Chinese Overseas Territories': 'CHN',
        'Japanese Overseas Territories': 'JPN',
        'Russian Overseas Territories': 'RUS',
        'Installations In International Waters': 'INT',
        'Petroleum Products': 'INT',
        'Neutral Zone': 'INT',
        'Curacao': 'CUW',
        'Jersey': 'JEY',
        'Pitcairn Islands': 'PCN',
        'Northern Mariana Islands': 'MNP',
        'Canary Islands': 'ESP',
        'Marshall Island': 'MHL',
        'Saharwi A.Dm Rp': 'ESH',
        'Tokelau Is': 'TKL',
        'Niue Is': 'NIU',
        'Channel Is': 'JEY',
        'Heard Macdonald': 'HMD',
        'Cocos Is': 'CCK',
        'Guernsey': 'GGY',
        'Sint Maarten (Dutch Part)': 'SXM',
        'Christmas Is.': 'CXR',
        'Fr S Ant Tr': 'ATF',
        'Falkland Is': 'FLK',
        'Saint Helena': 'SHN',
        'Ascension': 'SHN',
        'Tristan da Cunha': 'SHN',
        'Mayotte': 'MYT',
        'Reunion': 'REU',
        'Martinique': 'MTQ',
        'Guadeloupe': 'GLP',
        'French Guiana': 'GUF',
        'Saint Pierre': 'SPM',
        'Miquelon': 'SPM',
        'Wallis': 'WLF',
        'Futuna': 'WLF',
        'Saint Barthelemy': 'BLM',
        'Saint Martin': 'MAF',
        'Clipperton Is': 'CPT',
        'French Southern Territories': 'ATF',
        'British Indian Ocean Territory': 'IOT',
        'Akrotiri': 'CYP',
        'Dhekelia': 'CYP',
        'Sovereign Base Areas': 'CYP',
        'Mount Athos': 'GRC',
        'Campione d Italia': 'ITA',
        'Busan': 'KOR',
        'Incheon': 'KOR',
        'Jeju': 'KOR',
        'Okinawa': 'JPN',
        'Hokkaido': 'JPN',
        'Honshu': 'JPN',
        'Kyushu': 'JPN',
        'Shikoku': 'JPN',
        'Chiba': 'JPN',
        'Kanagawa': 'JPN',
        'Osaka': 'JPN',
        'Hyogo': 'JPN',
        'Aichi': 'JPN',
        'Saitama': 'JPN',
        'Fukuoka': 'JPN',
        'Hiroshima': 'JPN',
        'Sendai': 'JPN',
        'Kitakyushu': 'JPN',
        'Sakai': 'JPN',
        'Niigata': 'JPN',
        'Hamamatsu': 'JPN',
        'Kumamoto': 'JPN',
        'Sagamihara': 'JPN',
        'Shizuoka': 'JPN',
        'Okayama': 'JPN',
        'Hachioji': 'JPN',
        'Funabashi': 'JPN',
        'Kawaguchi': 'JPN',
        'Utsunomiya': 'JPN',
        'Matsuyama': 'JPN',
        'Higashiosaka': 'JPN',
        'Matsudo': 'JPN',
        'Nishinomiya': 'JPN',
        'Kurashiki': 'JPN',
        'Ichikawa': 'JPN',
        'Fukuyama': 'JPN',
        'Amagasaki': 'JPN',
        'Kanazawa': 'JPN',
        'Nagasaki': 'JPN',
        'Machida': 'JPN',
        'Gifu': 'JPN',
        'Himeji': 'JPN',
        'Mito': 'JPN',
        'Toyonaka': 'JPN',
        'Fukushima': 'JPN',
        'Toyota': 'JPN',
        'Takatsuki': 'JPN',
        'Yokosuka': 'JPN',
        'Toyohashi': 'JPN',
        'Nara': 'JPN',
        'Okazaki': 'JPN',
        'Suita': 'JPN',
        'Wakayama': 'JPN',
        'Asahikawa': 'JPN',
        'Koriyama': 'JPN',
        'Tokorozawa': 'JPN',
        'Kawagoe': 'JPN',
        'Akita': 'JPN',
        'Miyazaki': 'JPN',
        'Naha': 'JPN',
        'Takasaki': 'JPN',
        'Koshigaya': 'JPN',
        'Akashi': 'JPN',
        'Fujisawa': 'JPN',
        'Kochi': 'JPN',
        'Takamatsu': 'JPN',
        'Toyama': 'JPN',
        'Oita': 'JPN',
        'Nagano': 'JPN',
        'Iwaki': 'JPN',
        'Asaka': 'JPN',
        'Chochofu': 'JPN',
        'Kure': 'JPN',
        'Sasebo': 'JPN',
        'Hakodate': 'JPN',
        'Yao': 'JPN',
        'Ichihara': 'JPN',
        'Tokushima': 'JPN',
        'Hirakata': 'JPN',
        'Kashiwa': 'JPN',
        'Saga': 'JPN',
        'Suita': 'JPN',
        'Yamato': 'JPN',
        'Matsumoto': 'JPN',
        'Isesaki': 'JPN',
        'Kasugai': 'JPN',
        'Atsugi': 'JPN',
        'Yamagata': 'JPN',
        'Neyagawa': 'JPN',
        'Kofu': 'JPN',
        'Otsu': 'JPN',
        'Uji': 'JPN',
        'Fuji': 'JPN',
        'Takatsuki': 'JPN',
        'Tsukuba': 'JPN',
        'Odawara': 'JPN',
        'Maebashi': 'JPN',
        'Morioka': 'JPN',
        'Nagaoka': 'JPN',
        'Hitachi': 'JPN',
        'Suzuka': 'JPN',
        'Aomori': 'JPN',
        'Kamakura': 'JPN',
        'Yamaguchi': 'JPN',
        'Fuchu': 'JPN',
        'Kurume': 'JPN',
        'Matsue': 'JPN',
        'Tottori': 'JPN',
        'Kakogawa': 'JPN',
        'Fukui': 'JPN',
        'Mitaka': 'JPN',
        'Ota': 'JPN',
        'Tsu': 'JPN',
        'Shimonoseki': 'JPN',
        'Kisarazu': 'JPN',
        'Chigasaki': 'JPN',
        'Beppu': 'JPN',
        'Numazu': 'JPN',
        'Fujiidera': 'JPN',
        'Zama': 'JPN',
        'Sano': 'JPN',
        'Tachikawa': 'JPN',
        'Habikino': 'JPN',
        'Ube': 'JPN',
        'Obihiro': 'JPN',
        'Kashihara': 'JPN',
        'Izumi': 'JPN',
        'Kadoma': 'JPN',
        'Yamato': 'JPN',
        'Ome': 'JPN',
        'Sayama': 'JPN',
        'Komaki': 'JPN',
        'Warabi': 'JPN',
        'Seika': 'JPN',
        'Kunitachi': 'JPN',
        'Inagi': 'JPN',
        'Hamura': 'JPN',
        'Akiruno': 'JPN',
        'Nishitokyo': 'JPN',
        'Mizuho': 'JPN',
        'Hinode': 'JPN',
        'Hinohara': 'JPN',
        'Okutama': 'JPN',
        'Oshima': 'JPN',
        'Toshima': 'JPN',
        'Niijima': 'JPN',
        'Kozushima': 'JPN',
        'Miyake': 'JPN',
        'Mikurajima': 'JPN',
        'Hachijo': 'JPN',
        'Aogashima': 'JPN',
        'Ogasawara': 'JPN',
        'Chichijima': 'JPN',
        'Hahajima': 'JPN',
        'Minamitorishima': 'JPN',
        'Okinotorishima': 'JPN',
        'Akusekijima': 'JPN',
        'Suwanosejima': 'JPN',
        'Kuchinoerabujima': 'JPN',
        'Yakushima': 'JPN',
        'Tanegashima': 'JPN',
        'Magome': 'JPN',
        'Takegashima': 'JPN',
        'Shikinejima': 'JPN',
        'Jinjima': 'JPN',
        'Torishima': 'JPN',
        'Sofu Gan': 'JPN',
        'Bayonnaise Rocks': 'JPN',
        'Lotus Rocks': 'JPN',
        'Douglas Reef': 'JPN',
        'Smith Island': 'JPN',
        'Ryukyu Islands': 'JPN',
        'Senkaku Islands': 'JPN',
        'Diaoyu Islands': 'CHN',
        'Tiaoyutai': 'TWN',
        'Penghu': 'TWN',
        'Kinmen': 'TWN',
        'Matsu': 'TWN',
        'Pratas': 'TWN',
        'Taiping': 'TWN',
        'Zhongzhou': 'TWN',
        'Yongxing': 'CHN',
        'Qilian': 'CHN',
        'Shidao': 'CHN',
        'Yongshu': 'CHN',
        'Meiji': 'CHN',
        'Zhubi': 'CHN',
        'Huayang': 'CHN',
        'Nanxun': 'CHN',
        'Xiashu': 'CHN',
        'Dongsha': 'TWN',
        'Zhongsha': 'CHN',
        'Xisha': 'CHN',
        'Nansha': 'CHN',
        'Paracel': 'CHN',
        'Spratly': 'VNM',
        'Macclesfield Bank': 'CHN',
        'Scarborough Shoal': 'CHN',
        'Huangyan Dao': 'CHN',
        'Minamatori Shima': 'JPN',
        'Marcus Island': 'JPN',
        'Okinotori Shima': 'JPN',
        'Parece Vela': 'JPN',
        'Virgen del Carmen': 'JPN',
        'Ganges': 'JPN',
        'Kita no Ojima': 'JPN',
        'Minami Kojima': 'JPN',
        'Uotsuri Shima': 'JPN',
        'Daio Shima': 'JPN',
        'Kuba Shima': 'JPN',
        'Taisho Shima': 'JPN',
        'Sekibi Shima': 'JPN',
        'Nishi Shima': 'JPN',
        'Higashi Shima': 'JPN',
        'Kita Shima': 'JPN',
        'Minami Shima': 'JPN',
        'O Shima': 'JPN',
        'Ko Shima': 'JPN',
        'Tori Shima': 'JPN',
        'Yonaguni': 'JPN',
        'Iriomote': 'JPN',
        'Ishigaki': 'JPN',
        'Miyako': 'JPN',
        'Yaeyama': 'JPN',
        'Amami': 'JPN',
        'Tokara': 'JPN',
        'Osumi': 'JPN',
        'Satsuma': 'JPN',
        'Koshikijima': 'JPN',
        'Kamikoshiki': 'JPN',
        'Shimokoshiki': 'JPN',
        'Kuro Shima': 'JPN',
        'Iwo Jima': 'JPN',
        'Ioto': 'JPN',
        'Minami Ioto': 'JPN',
        'Kita Ioto': 'JPN',
        'Iwo To': 'JPN',
        'Kazan Retto': 'JPN',
        'Volcano Islands': 'JPN',
        'Bonin Islands': 'JPN',
        'Ogasawara Gunto': 'JPN',
        'Munin To': 'JPN',
        'Anijima': 'JPN',
        'Chichijima Retto': 'JPN',
        'Hahajima Retto': 'JPN',
        'Imotojima': 'JPN',
        'Nishinotima': 'JPN',
        'Minamidaito': 'JPN',
        'Kitadaito': 'JPN',
        'Okidaito': 'JPN',
        'Daito': 'JPN',
        'Borodino': 'RUS',
        'Simushir': 'RUS',
        'Ketoy': 'RUS',
        'Urup': 'RUS',
        'Iturup': 'RUS',
        'Kunashir': 'RUS',
        'Shikotan': 'RUS',
        'Habomai': 'RUS',
        'Etorofu': 'JPN',
        'Kunashiri': 'JPN',
        'Shikotan': 'JPN',
        'Habomai': 'JPN',
        'Chishima': 'JPN',
        'Hoppo': 'JPN',
        'Senhaku': 'JPN',
        'Takeshima': 'JPN',
        'Ulleungdo': 'KOR',
        'Dokdo': 'KOR',
        'Liancourt Rocks': 'KOR',
        'Dagelet': 'KOR',
        'Argonaut': 'KOR',
        'Hornet': 'KOR',
        'Okinotorishima': 'JPN',
        'Parece Vela': 'JPN',
        'Ganges': 'JPN',
        'Hateruma': 'JPN',
        'Yonaguni Jima': 'JPN',
    }
    
    # Map trade countries to ISO-3
    trade_mapped = trade_df.copy()
    trade_mapped['iso_country'] = trade_mapped['Country'].map(country_to_iso)
    
    # Fill unmapped with original (might already be ISO-3)
    trade_mapped['iso_country'] = trade_mapped['iso_country'].fillna(trade_mapped['Country'])
    
    # Group by ISO-3 and take max Trade_Risk (conservative approach)
    trade_iso = trade_mapped.groupby('iso_country')['Trade_Risk'].max().reset_index()
    trade_iso.columns = ['Country', 'Trade_Risk']
    
    print(f"   ✅ Mapped {len(trade_iso)} countries to ISO-3")
    
    # ================================================================
    # STEP 3: HANDLE CLIMATE DATA (INDIAN STATES → IND)
    # ================================================================
    
    # Check if climate data contains Indian states
    indian_states_keywords = ['Andhra', 'Bihar', 'Gujarat', 'Karnataka', 'Kerala', 
                              'Maharashtra', 'Tamil', 'Uttar', 'West Bengal', 'Rajasthan',
                              'Madhya', 'Telangana', 'Assam', 'Punjab', 'Haryana',
                              'Himachal', 'Uttarakhand', 'Jharkhand', 'Chhattisgarh',
                              'Odisha', 'Tripura', 'Meghalaya', 'Manipur', 'Nagaland',
                              'Goa', 'Arunachal', 'Mizoram', 'Sikkim', 'Delhi']
    
    is_india_climate = any(any(keyword in str(state) for keyword in indian_states_keywords) 
                          for state in climate_country['Country'].unique())
    
    if is_india_climate:
        print("\n   ⚠️  Climate data contains ONLY Indian states.")
        print("      Skipping climate data for global interconnection...")
        print("      (Trade + Geopolitics will be used instead)")
        
        # Return empty climate dataframe to signal skipping
        climate_country = pd.DataFrame({'Country': [], 'climate_risk': []})
    else:
        # Try to map other countries to ISO-3
        climate_country['iso_country'] = climate_country['Country'].map(country_to_iso)
        climate_country['iso_country'] = climate_country['iso_country'].fillna(climate_country['Country'])
        climate_country = climate_country.groupby('iso_country')['climate_risk'].mean().reset_index()
        climate_country.columns = ['Country', 'climate_risk']
    
    # ================================================================
    # STEP 4: MERGE ALL DATASETS (INNER JOIN ON ISO-3)
    # ================================================================
    
    print("\n   🔗 Performing sequential merges on ISO-3 country codes...")
    
    # Check if climate data is empty (India-only case)
    if len(climate_country) == 0:
        print("      ⚠️  Climate skipped - using Trade + Geopolitics only")
        # Merge trade and geopolitics only
        merged_df = pd.merge(trade_iso, geopolitics_clean, on='Country', how='inner')
        print(f"   ✅ Trade-Geopolitics match: {len(merged_df)} countries")
        
        # Add placeholder climate column
        merged_df['climate_risk'] = np.nan
    else:
        # Step 1: Merge climate and trade
        merged_df = pd.merge(climate_country, trade_iso, on='Country', how='inner')
        print(f"   ✅ Climate-Trade match: {len(merged_df)} countries")
        
        # Step 2: Merge with geopolitics
        merged_df = pd.merge(merged_df, geopolitics_clean, on='Country', how='inner')
    
    print(f"   ✅ Climate-Trade-Geopolitics match: {len(merged_df)} countries")
    
    if len(merged_df) > 0:
        print(f"      Mean Climate Risk: {merged_df['climate_risk'].mean():.4f}")
        print(f"      Mean Trade Risk: {merged_df['Trade_Risk'].mean():.4f}")
        print(f"      Mean Geopolitics Risk: {merged_df['geopolitics_risk'].mean():.4f}")
    else:
        print("\n   ⚠️  WARNING: No matching countries found!")
        print("      Sample climate countries:", climate_country['Country'].head().tolist())
        print("      Sample trade countries:", trade_iso['Country'].head().tolist())
        print("      Sample geopolitics countries:", geopolitics_clean['Country'].head().tolist())
    
    return merged_df


# ================================================================
# 🔧 CALCULATE INTERCONNECTED RISK
# ================================================================

def calculate_interconnected_risk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate interconnected risk scores combining climate, trade, and geopolitics risks.
    
    This function creates a unified risk score by:
    1. Normalizing each sector to [0, 1] scale (CRITICAL for fair weighting!)
    2. Computing weighted average of all three sector risks
    3. Applying cascading risk multipliers for compound hazards
    4. Normalizing final risk scores to [0, 1] range
    
    WHY NORMALIZE FIRST?
    Climate risk range: [0.02, 0.24] - Very compressed!
    Trade risk range:   [0.01, 0.83] - Full spread
    Geo risk range:     [0.03, 0.93] - Full spread
    
    Without normalization, climate would be marginalized despite 30% weight.
    
    Interconnection weights (equal importance after normalization):
    - Climate: 33.3%
    - Trade: 33.3%
    - Geopolitics: 33.4%
    
    Args:
        df (pd.DataFrame): DataFrame with 'climate_risk', 'Trade_Risk', and 'geopolitics_risk' columns
    
    Returns:
        pd.DataFrame: DataFrame with interconnected risk scores
    """
    df = df.copy()
    
    # Check if climate data is available
    has_climate = df['climate_risk'].notna().any()
    
    if has_climate:
        print("\n🔮 Calculating interconnected risks (Climate + Trade + Geopolitics)...")
    else:
        print("\n🔮 Calculating interconnected risks (Trade + Geopolitics only)...")
        print("   ⚠️  Climate data unavailable - using 50/50 weights")
    
    # ================================================================
    # 🔧 STEP 0: NORMALIZE EACH SECTOR TO [0, 1] (CRITICAL FIX!)
    # ================================================================
    print("   ⚠️  CRITICAL: Normalizing sectors before combination...")
    
    # Normalize each sector independently
    for sector_col in ['climate_risk', 'Trade_Risk', 'geopolitics_risk']:
        min_val = df[sector_col].min()
        max_val = df[sector_col].max()
        
        if pd.isna(min_val) or pd.isna(max_val):
            # Column is all NaN, skip
            df[f'{sector_col}_norm'] = np.nan
        elif max_val - min_val > 0:
            df[f'{sector_col}_norm'] = (df[sector_col] - min_val) / (max_val - min_val)
        else:
            df[f'{sector_col}_norm'] = df[sector_col]
    
    if has_climate:
        print(f"      Climate range before: [{df['climate_risk'].min():.4f}, {df['climate_risk'].max():.4f}]")
    else:
        print(f"      Climate: SKIPPED (all NaN)")
    print(f"      Trade range before:   [{df['Trade_Risk'].min():.4f}, {df['Trade_Risk'].max():.4f}]")
    print(f"      Geo range before:     [{df['geopolitics_risk'].min():.4f}, {df['geopolitics_risk'].max():.4f}]")
    print(f"      All sectors normalized to [0, 1] ✅")
    
    # ================================================================
    # 🔧 STEP 1: WEIGHTED RISK (using normalized values)
    # ================================================================
    print("\n   📊 Calculating weighted risk...")
    
    # Adjust weights based on available data
    if has_climate:
        # Equal weights: 33.3% each
        df['weighted_risk'] = (0.333 * df['climate_risk_norm'] + 
                              0.333 * df['Trade_Risk_norm'] + 
                              0.334 * df['geopolitics_risk_norm'])
        print("   ✅ Weighted risk calculated (Climate:33.3%, Trade:33.3%, Geopolitics:33.4%)")
    else:
        # No climate: 50% Trade, 50% Geopolitics
        df['weighted_risk'] = (0.5 * df['Trade_Risk_norm'] + 
                              0.5 * df['geopolitics_risk_norm'])
        print("   ✅ Weighted risk calculated (Trade:50%, Geopolitics:50%)")
    
    # ================================================================
    # 🔧 STEP 2: CASCADING RISK (using normalized values)
    # ================================================================
    print("\n   🔗 Calculating cascading risk multiplier...")
    
    # Compound hazard multiplier: if 2+ sectors have high risk (>0.6), amplify by 25%
    # Now using normalized values so threshold is meaningful across all sectors
    if has_climate:
        df['high_risk_count'] = ((df['climate_risk_norm'] > 0.6).astype(int) + 
                                (df['Trade_Risk_norm'] > 0.6).astype(int) + 
                                (df['geopolitics_risk_norm'] > 0.6).astype(int))
    else:
        # Only 2 sectors available
        df['high_risk_count'] = ((df['Trade_Risk_norm'] > 0.6).astype(int) + 
                                (df['geopolitics_risk_norm'] > 0.6).astype(int))
    
    df['cascading_risk'] = np.where(
        df['high_risk_count'] >= 2,
        df['weighted_risk'] * 1.25,  # 25% amplification for compound risks
        df['weighted_risk']
    )
    print("   ✅ Cascading risk calculated (compound hazard multiplier: 25% when 2+ sectors high)")
    
    # ================================================================
    # 🔧 STEP 3: FINAL NORMALIZATION TO [0, 1]
    # ================================================================
    print("\n   ⚖️  Normalizing final risk to [0, 1] range...")
    
    min_risk = df['cascading_risk'].min()
    max_risk = df['cascading_risk'].max()
    
    # Avoid division by zero
    if max_risk - min_risk > 0:
        df['final_risk'] = (df['cascading_risk'] - min_risk) / (max_risk - min_risk)
    else:
        df['final_risk'] = df['cascading_risk']
    
    print("   ✅ Final risk normalized to [0, 1] range")
    
    # Print summary statistics
    print(f"\n   📊 Final Risk Statistics:")
    print(f"      Min: {df['final_risk'].min():.4f}")
    print(f"      Max: {df['final_risk'].max():.4f}")
    print(f"      Mean: {df['final_risk'].mean():.4f}")
    
    return df


def get_risk_level(score: float) -> str:
    """
    Classify risk level based on normalized score (0-1).
    
    Args:
        score (float): Normalized risk score (0.0 to 1.0)
    
    Returns:
        str: Risk level category
    """
    if score < 0.05:
        return "VERY LOW"
    elif score < 0.10:
        return "LOW"
    elif score < 0.20:
        return "MEDIUM"
    elif score < 0.40:
        return "HIGH"
    else:
        return "VERY HIGH"

# ================================================================
# 🚀 RUN ENGINE
# ================================================================

def run_interconnection():
    """
    Run the interconnection engine combining climate, trade, and geopolitics risks.
    
    Returns:
        pd.DataFrame: DataFrame with all interconnected risk scores
    """
    print("\n" + "="*60)
    print("🌍 RUNNING GLOBAL RISK INTERCONNECTION ENGINE (CLIMATE + TRADE + GEOPOLITICS)")
    print("="*60)
    
    # Step 1: Aggregate climate to country level
    climate_country = aggregate_climate_to_country(climate_df)
    
    # Step 2: Merge datasets (climate + trade + geopolitics)
    merged_df = merge_risk_datasets(climate_country, trade_df, geopolitics_df)
    
    # Step 3: Handle missing values
    # Only drop rows where BOTH trade and geopolitics are NaN
    # Climate can be NaN (India-only case)
    required_cols = ['Trade_Risk', 'geopolitics_risk']
    merged_df = merged_df.dropna(subset=required_cols)
    print(f"\n🔧 Cleaned dataset: {len(merged_df)} countries (after dropping NaN)")
    
    # Step 4: Calculate interconnected risk
    df = calculate_interconnected_risk(merged_df)
    
    # Step 5: Apply risk level classification
    df['risk_level'] = df['final_risk'].apply(get_risk_level)
    print("   ✅ Risk levels classified")
    
    print("\n📊 INTERCONNECTION ANALYSIS COMPLETE")
    print("-" * 60)
    print(f"   Total countries analyzed: {len(df)}")
    print(f"   Mean Climate Risk: {df['climate_risk'].mean():.4f}")
    print(f"   Mean Trade Risk: {df['Trade_Risk'].mean():.4f}")
    print(f"   Mean Geopolitics Risk: {df['geopolitics_risk'].mean():.4f}")
    print(f"   Mean Final Risk: {df['final_risk'].mean():.4f}")
    print("="*60)
    
    return df

# ================================================================
# 💾 SAVE OUTPUT
# ================================================================

def save_output(df: pd.DataFrame):
    """
    Save interconnected risk data to CSV.
    
    Args:
        df (pd.DataFrame): DataFrame with interconnected risk scores
    """
    output_path = os.path.join(
        BASE_PATH, 
        "data", 
        "processed", 
        "interconnection", 
        "global_risk.csv"
    )
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Select relevant columns for output (includes normalized and original values)
    output_df = df[['Country', 'climate_risk', 'climate_risk_norm', 
                    'Trade_Risk', 'Trade_Risk_norm',
                    'geopolitics_risk', 'geopolitics_risk_norm',
                    'weighted_risk', 'cascading_risk', 'final_risk', 'risk_level']].copy()
    
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Interconnection data saved → {output_path}")
    
    # Display top 10 high-risk countries
    print("\n🔴 TOP 10 HIGHEST RISK COUNTRIES:")
    print("-" * 80)
    top_10 = df.nlargest(10, 'final_risk')
    for idx, row in top_10.iterrows():
        print(f"   {row['Country']:35s} | Final: {row['final_risk']:.4f} ({row['risk_level']})")
        print(f"      Climate: {row['climate_risk']:.4f}→{row['climate_risk_norm']:.4f} | "
              f"Trade: {row['Trade_Risk']:.4f}→{row['Trade_Risk_norm']:.4f} | "
              f"Geo: {row['geopolitics_risk']:.4f}→{row['geopolitics_risk_norm']:.4f} | "
              f"Weighted: {row['weighted_risk']:.4f} | Cascading: {row['cascading_risk']:.4f}")
        print()

# ================================================================
# 🎯 MAIN EXECUTION
# ================================================================

if __name__ == "__main__":
    df = run_interconnection()
    save_output(df)
    
    print("\n" + "="*60)
    print("🎉 INTERCONNECTION ENGINE EXECUTION COMPLETE")
    print("="*60)
