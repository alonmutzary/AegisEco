import requests
import time

class AegisEcoDataIngestor:
    def __init__(self):
        self.base_url = "https://www.weather2day.co.il/clientfiles/public/floods.json"
        
        # Translating Hebrew to English basin names
        self.station_mapping = {
            "חרוד-כביש 90": "Harod",
            "שורק-יבנה": "Sorek - lower",
            "שורק-יסודות": "Sorek - lower", 
            "שורק-מוצא": "Shorek - upper",
            "שורק הר טוב": "Shorek - upper",
            "שורק-כביש גדרה בילו": "Sorek - lower",
            "דישון-כביש ראש פינה-מטולה": "Dishon",
            "ירדן דגניה": "Upper Jordan River-south west",
            "ירדן-שדה נחמיה": "Upper Jordan River-north west",
            "ירדן-גשר הפקק": "Upper Jordan River-east",
            "ירדן-אתר טבילה": "Upper Jordan River-east",
            "ירדן-בית זרע": "Jordan Naharayim",
            "ירדן - גשר": "Jordan Naharayim",
            "ירקון-כביש להרצליה": "Yarkon - lower",
            "דן שמורת טבע": "Dan",
            "חרמון-כביש לכפר סאלד": "Hermon",
            "שניר מעין ברוך": "Snir",
            "שניר-כביש לדן": "Snir",
            "בשור-רעים": "Besor -lower",
            "יהודיה-בית צידה": "Yehudiya",
            "חיון": "Hayon - upper",
            "צלמון": "Tzalmon",
            "איילון נתבג": "Yarkon - upper, Ayalon",
            "איילון - לוד": "Yarkon - upper, Ayalon",
            "קישון-מחצבה": "Kishon - lower",
            "באר שבע-חצרים": "Besor - upper, Beer Sheva",
            "נטוף-אל על": "Yarkon - upper, Shilo",
            "דליה בת שלמה": "Dalya",
            "דליה כביש תא-חיפה": "Dalya",
            "חדרה - גן שמואל": "Hadera",
            "עיון-מטולה": "Upper Jordan River-north west",
            "אלכסנדר-אליישיב": "Alexander",
            "משושים-דרדרה": "Meshushim",
            "שקמה -ארז": "Shikma - lower",
            "שקמה-ברור חייל": "Shikma - upper",
            "ציפורי-תל עליל": "Kishon - upper, Zepuri",
            "עורבים-להבות הבשן": "Upper Jordan River-east",
            "רחף": "Rahaf",
            "אשלים במעלה": "Ashalim",
            "תעלת אשלים": "Ashalim",
            "יבניאל": "Yavneel",
            "געתון - בן עמי ח.": "Gaaton",
            "דישון-כביש ראש פינה-מטולה": "Dishon",
            "עמוד": "Amud",
            "לכיש-עד הלום": "Lachish - Sourek",
            "דליות-בית צידה": "Daliyot",
            "חילזון-יסעור": "Naaman - upper",
            "צין - כביש הערבה": "Tsin -lower",
            "צאלים": "Zeelim",
            "ערוגות": "Arugot",
            "אמציהו- במעלה עין תמר": "Amatsyahu",
            "שילה - כביש לוד-ראש העין": "Yarkon - upper, Shilo",
            "שילה במורד הזרימה": "Yarkon - upper, Shilo",
            "שילה במעלה": "Yarkon - upper, Shilo",
            "נחל חברון": "Besor - upper, Beer Sheva",
            "האלה - גן יבנה": "Lachish - Ela",
            "האלה-תל צפית": "Lachish - Ela",
            "קנה-ירחיב": "Yarkon - upper, Kana",
            "עדה-גבעת עדה": "Hadera",
            "תנינים עמיקם": "Taninim",
            "בצת - כביש 4": "Betzet",
            "כזיב": "Kziv",
            "סמך": "Samach",
            "יסף": "Yasaf",
            "עמרם": "Amram"
        }

    def fetch_live_data(self):
            print("[API Client] Fetching live river flows via API...")
            
            current_timestamp = int(time.time() * 1000)
            dynamic_url = f"{self.base_url}?_={current_timestamp}"
            
            rivers_data = {}
            
            try:
                response = requests.get(dynamic_url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                for station in data:
                    raw_name = station["name"]
                    if '\\u' in raw_name:
                        hebrew_name = raw_name.encode('utf-8').decode('unicode_escape')
                    else:
                        hebrew_name = raw_name

                    # Check if we have a mapping to the English name, otherwise skip the station
                    english_name = self.station_mapping.get(hebrew_name)
                    
                    if english_name:
                        for obs in station.get("obs", []):
                            if obs.get("flow") not in (None, ""):
                                # In case multiple stations mapped to the same English name, keep the maximum flow
                                current_flow = float(obs["flow"])
                                if english_name not in rivers_data or current_flow > rivers_data[english_name]["flow_m3s"]:
                                    rivers_data[english_name] = {
                                        "flow_m3s": current_flow,
                                        "last_updated": f"{obs.get('date')} {obs.get('time')}",
                                        "original_hebrew": hebrew_name
                                    }
                                break 
                
                print(f"[API Client] Extracted data for {len(rivers_data)} mapped stations.")
                return rivers_data

            except Exception as e:
                print(f"[API Client Error]: {e}")
                return {}

# Test Run
if __name__ == "__main__":
    ingestor = AegisEcoDataIngestor()
    live_map_data = ingestor.fetch_live_data()
    
    print("\n=== AEGISECO LIVE API DATA (MAPPED TO ENGLISH) ===")
    
    sorted_rivers = sorted(live_map_data.items(), key=lambda item: item[1]["flow_m3s"], reverse=True)
    
    for eng_river, data_dict in sorted_rivers:
        flow = data_dict["flow_m3s"]
        print(f"{eng_river:<35} | {flow:>5.1f} m3/s")
        
    print("-" * 50)
    print(f"Total mapped stations online: {len(live_map_data)}")