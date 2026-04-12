BASIN_ROUTING_TABLE = {
    # 1. North and Upper Galilee (model deals with high mountains, strong flow)
    "Dishon": [
        "Dishon", "Dan", "Hermon", "Snir", "Amud", "Or Koach", "Korazim", 
        "Upper Jordan River-east", "Upper Jordan River-north west", "Upper Jordan River-south west",
        "Kinneret-south-east", "Local Drain - Kinneret 1", "Local Drain - Kinneret 2",
        "Kiner", "Samach", "Meshushim", "Yehudiya", "Daliyot", "Kanaf", "Orvim" # Golan Heights and Sea of Galilee basins
    ],

    # 2. Western Galilee and North Coast (drainage to the Mediterranean from the Galilee mountains)
    "Keziv": [
        "Kziv", "Betzet", "Gaaton", "Beit HaEmek", "Yasaf", "Shaal"
    ],

    # 3. Haifa area, Krayot, and northern valleys
    "Kishon": [
        "Kishon - lower", "Kishon - upper", "Kishon - upper, Zepuri", 
        "Naaman - lower", "Naaman - upper"
    ],

    # 4. Jezreel Valley, Valley of Springs, and northern Jordan Valley
    "Harod": [
        "Harod", "Beit Shean", "Tavor", "Yisachar", 
        "Yarmouk  - north west", "Yarmouk - south west", "Jordan Naharayim"
    ],

    # 5. Carmel Coast and northern Sharon
    "Taninim": [
        "Taninim", "Dalya", "Mearot", "Oren", "Carmel-north-west"
    ],

    # 6. Hefer Valley and central Sharon
    "Hadera": [
        "Hadera"
    ],
    
    "Alexander": [
        "Alexander", "Poleg"
    ],

    # 7. Gush Dan, southern Sharon, and Samaria (West)
    "Yarkon": [
        "Yarkon - lower", "Yarkon - upper, Kana", "Yarkon - upper, Shilo", "Yarkon - upper, Dulave",
        "Tirza", "Yitav", "Bezeq", "Abu Sidra", "A-Naima", "Al-Ahmar" # Central Jordan Valley - affected by Samaria rainfall
    ],
    
    # Specific model for the heart of Gush Dan
    "Ayalon": [
        "Yarkon - upper, Ayalon"
    ],

    # 8. Jerusalem Mountains, Shfela, and central coastal plain
    "Sorek": [
        "Sorek - lower", "Shorek - upper", "Lachish - Sourek"
    ],

    # 9. Southern Shfela and Ashdod/Ashkelon coast
    "Lachish": [
        "Lachish - Ela", "Lachish - Govrin", "Evtakh", "Shvarim"
    ],

    # 10. North-western Negev (Gaza envelope)
    "Shikma": [
        "Shikma - lower", "Shikma - upper"
    ],
    
    "Gerar": [
        "Besor - upper, Besor", "Besor -lower", "El Arish Partial", "Hevel Shalom-Rafah Partial", "Egypt streams -1", "Egypt streams -2", "Egypt streams -3"
    ],

    # 11. Northern Negev, Beer Sheva, and Mount Hebron
    "Beer Sheva": [
        "Besor - upper, Beer Sheva", "Lavan", "Omer-Shvia-Erga"
    ],

    # 12. Dead Sea and Judean Desert (model dealing with extreme elevation drops to the Dead Sea)
    "Zin": [
        "Tsin - upper", "Tsin -lower", "Arugot", "David", "Darga", "Masada Region", 
        "Bokek Region", "Rahaf", "Hever", "Khatsatson", "Kane - Samar Springs", "Kidron", 
        "Kumeran", "Shalem - Berniky", "Og", "Prat", "Zeelim", "Mishmar", "Hemar", "Ashalim",
        "Local Drain - Ded sea, 1", "Local Drain - Ded sea, 2"
    ],

    # 13. Arava and Southern Negev (massive basins, desert soil)
    "Paran": [
        "Paran - upper, Arod", "Paran - upper, Tsihor", "Paran - drain to Egypt", "Paran - lower",
        "Hayon - upper", "Hayon - lower", "Menuha", "Ashosh", "Barak", "Amatsyahu", "Sheizaf", "Idan",
        "Zihor", "Yaalon Shita", "Ktora", "Nehushtan", "Local Drain - Arava", "Local Drain - Arava, 3",
        "Local Drain - Arava,1", "Local Drain -Arava, 2",
        "Eilat streams", "Amram", "Roded", "Shehoret", "Evrona streams", "Timna", "Yotvata streams",
        "Hemda", "Sfamnun", "Malkha", "Miflat"
    ]
}