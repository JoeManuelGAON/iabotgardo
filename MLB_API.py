import requests
import datetime
import mysql.connector
from pymongo import MongoClient
config1 = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'mysql',
}
config = {
    'user': 'sudo',
    'password': 'Server23!',
    'host': 'db-price.cocgnquyre0g.us-east-2.rds.amazonaws.com',
    'database': 'mkp2',
}
# Establecer la conexión con la API de Mercado Libre
access_token = "APP_USR-3982796084172307-050417-cbcf49aeae7c05ac6863d6082f463151-609389670"

try:
    cnn = mysql.connector.connect(**config)
    print('Connected to database')

    # Perform database operations here
except mysql.connector.Error as err:
    print('Error connecting to database:', err)
def inserta_actualiza_categoria(cnn,access_token):
    cursor = cnn.cursor()
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.mercadolibre.com/sites/MLM/categories"
    response = requests.get(url, headers=headers)
    data = response.json()
    for category in data:
        Categoria = category["id"]
        Nombre = category["name"]
        query = f"INSERT INTO Categories(ID_Category,Name_Category) VALUES (%s, %s) ON DUPLICATE KEY UPDATE Name_Category = %s"
        cursor.execute(query, (Categoria, Nombre, Nombre))
    cnn.commit()
    cursor.close()
def inserta_actualiza_tienda(cnn):
    cursor = cnn.cursor()
    url = f"https://api.mercadolibre.com/users/609389670/brands"
    response = requests.get(url)
    data = response.json()
    #print(f"Total de productos devueltos por la API: {total_products}")
    for tienda in data["brands"]:
        store_id = tienda["official_store_id"]
        nombre = tienda["name"]
        fantasy_name = tienda["fantasy_name"]
        status=tienda["status"]
        permalink=tienda["permalink"]
        query = f"INSERT INTO Store(Official_Store_ID,Name_Store,Fantasy_name,Status_Store,Permalink) VALUES (%s, %s,%s,%s,%s) ON DUPLICATE KEY UPDATE Name_Store= %s,Fantasy_name= %s,Status_Store= %s,Permalink= %s "
        cursor.execute(query, (store_id, nombre,fantasy_name,status,permalink,nombre,fantasy_name,status,permalink))
    cnn.commit()
    cursor.close()

def get_or_insert_category(cursor, ID_Category, Name_Category):
    # Consulta para obtener la categoría utilizando Categoria_id
    check_category_query = "SELECT * FROM Categories WHERE ID_Category = %s"
    cursor.execute(check_category_query, (ID_Category,))
    result = cursor.fetchone()
    # Si la categoría no existe, insertarla en la tabla Categories
    if not result:
        insert_category_query = "INSERT INTO Categories (ID_Category, Name_Category) VALUES (%s, %s)"
        cursor.execute(insert_category_query, (ID_Category, Name_Category))
        cursor._connection.commit()
def get_item_details(item_id, headers):
    url = f"https://api.mercadolibre.com/items/{item_id}"
    response = requests.get(url, headers=headers)
    data = response.json()
    return data
def consulta_productos(cnn,access_token):
    cursor = cnn.cursor()
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://api.mercadolibre.com/sites/MLM/search?seller_id=609389670"
    all_products = []
    total_products=0
    while url:
        response = requests.get(url, headers=headers)
        data = response.json()
        all_products.extend(data["results"])
        total_products += len(data["results"])
        if data['paging']['total'] > data['paging']['offset'] + data['paging']['limit']:
            offset = data['paging']['offset'] + data['paging']['limit']
            #url = f"https://api.mercadolibre.com/sites/MLM/search?seller_id=609389670&offset={offset}"
            url = f"https://api.mercadolibre.com/sites/MLM/search?seller_id=609389670&offset={offset}&limit=50"
        else:
            url = None
    for product in all_products:
        #print(a)
        MLM = product["id"]
        ID_Category = product["category_id"]
        Title = product["title"]
        Original_Price = product["original_price"]
        Offer_Price = product["price"]
        Permalink = product["permalink"]
        item_details = get_item_details(product["id"], headers)
        Status_Product = item_details.get("status")
        Oficial_Store_ID=item_details.get("official_store_id")
        Available_Quantity=item_details.get("available_quantity")
        Sold_Quantity=item_details.get("sold_quantity")
        category_name_url = f"https://api.mercadolibre.com/categories/{ID_Category}"
        response = requests.get(category_name_url)
        category_data = response.json()
        Category_Name = category_data["name"]
        get_or_insert_category(cursor, ID_Category, Category_Name)
        if Offer_Price > 0 and Offer_Price < Original_Price:
            Campaign = "SI"
        else:
            Campaign = "NO"
        if Offer_Price == None:
            Offer_Price = 0
        # Get item details including status
        check_query = "SELECT COUNT(*) FROM Products_MLB WHERE MLM = %s "
        cursor.execute(check_query, (MLM,))
        product_count = cursor.fetchone()[0]
        if product_count > 0:
            update_query= "UPDATE Products_MLB SET ID_Category = %s,Official_Store_ID = %s,Title_Products = %s,Offer_Price = %s,Original_Price = %s,Sold_Quantity = %s,Available_Quantity = %s,Permalink = %s,Campaign = %s,Status_Product = %s WHERE MLM = %s"
            cursor.execute(update_query, (ID_Category, Oficial_Store_ID, Title, Offer_Price, Original_Price, Sold_Quantity, Available_Quantity, Permalink, Campaign, Status_Product, MLM))
        #query = f"INSERT INTO Prod_MLB(MLM,Nombre,Categoria,Precio_Original,Precio_Oferta,Cantidad_Stock,Cantidad_Vendida,Descripcion,Campaña,Estado) VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s)"
        #cursor.execute(query, (MLM, nombre, categoria, precio_original, precio_oferta, cantidad_stock, cantidad_vendida, permalink, campaña,estado))
        else:
            query = f"INSERT INTO Products_MLB(MLM,ID_Category,Official_Store_ID,Title_Products,Offer_Price,Original_Price,Sold_Quantity,Available_Quantity,Permalink,Campaign,Status_Product) VALUES (%s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, (MLM,ID_Category,Oficial_Store_ID,Title,Offer_Price,Original_Price,Sold_Quantity,Available_Quantity,Permalink,Campaign,Status_Product))
    cnn.commit()
    cursor.close()
#inserta_actualiza_tienda(cnn)
#inserta_actualiza_categoria(cnn,access_token)
consulta_productos(cnn,access_token)
