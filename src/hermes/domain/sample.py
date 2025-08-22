class Sample:
    STATES_AND_CITIES = {
        "state,city": "keys",
        "ar-a,*": "Salta",
        "ar-b,*": "Buenos Aires",
        "ar-c,*": "CABA",
        "ar-d,*": "San Luis",
        "ar-e,*": "Entre Rios",
        "ar-f,*": "La Rioja",
        "ar-g,*": "Santiago del Estero",
        "ar-h,*": "Chaco",
        "ar-j,*": "San Juan",
        "ar-k,*": "Catamarca",
        "ar-l,*": "La Pampa",
        "ar-m,*": "Mendoza",
        "ar-n,*": "Misiones",
        "ar-p,*": "Formosa",
        "ar-q,*": "Neuquén",
        "ar-r,*": "Río Negro",
        "ar-s,*": "Santa Fé",
        "ar-t,*": "Tucumán",
        "ar-u,*": "Chubut",
        "ar-v,*": "Tierra del Fuego",
        "ar-w,*": "Corrientes",
        "ar-x,*": "Córdoba",
        "ar-y,*": "San Salvador de Jujuy",
        "ar-z,*": "Santa Cruz",
        "buenos aires,*": "Buenos Aires",
        "catamarca,*": "Catamarca",
        "chaco,*": "Chaco",
        "cordoba,*": "Córdoba",
        "corrientes,*": "Corrientes",
        "la pampa,*": "La Pampa",
        "la rioja,*": "La Rioja",
        "neuquen,*": "Neuquén",
        "rio negro,*": "Río Negro",
        "salta,*": "Salta",
        "san luis,*": "San Luis",
        "santa fe,*": "Santa Fe",
        "santiago del estero,*": "Santiago del Estero",
        "tucuman,*": "Tucumán",
        "caba,*": "CABA",
        "jujuy,*": "Jujuy",
        "formosa,*": "Formosa",
        "misiones,*": "Misiones",
        "capital federal,*": "CABA",
        "mendoza,*": "Mendoza",
        "error,*": "Error",
    }

    MECON = "mecon"
    PARAMETERS = "parameters"
    TREE_STORE = "tree_store"
    DATABASE = "database"  # directory name
    PARAMETERS = "parameters"
    REPORTS = "reports"
    STATES_AND_CITIES_SELECTOR = "states_and_cities_selector"
    ARTICLES_SELECTOR = "articles_selector"
    POINT_OF_SALE_CODE = "point_of_sale_code"
    STATE = "state"
    CITY = "city"
    ADDRESS = "address"
    PLACE = "place"
    FLAG = "flag"
    BUSINESS = "business"
    BRANCH = "branch"
    CITY_KEY = "city_key"
    PLACE_KEY = "place_key"
    POINT_OF_SALE_KEY = "point_of_sale_key"
    ARTICLE_CODE = "article_code"
    BRAND = "brand"
    DESCRIPTION = "description"
    PRICE = "price"
    POINTS_OF_SALE = "points_of_sale"
    ARTICLES = "articles"

    expected_points_of_sale_keys = [
        POINT_OF_SALE_CODE,
        STATE,
        CITY,
        ADDRESS,
        FLAG,
        BUSINESS,
        BRANCH,
        CITY_KEY,
        PLACE_KEY,
        POINT_OF_SALE_KEY,
    ]
