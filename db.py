import mysql.connector
from mysql.connector import errorcode
from api_football import get_competition, get_teams, get_players


#  Constantes
DB_NAME = "leagues"
TABLES = {}

#Tablas iniciales
TABLES['competition'] = (
    """ CREATE TABLE competition (id INT PRIMARY KEY, code VARCHAR(15), name VARCHAR(50), areaName VARCHAR(30)) ENGINE=InnoDB; """
)
TABLES['team'] = (
    """ CREATE TABLE team (id INT PRIMARY KEY, name VARCHAR(50), tla VARCHAR(5), shortName VARCHAR(25), areaName VARCHAR(30), email VARCHAR(70)) ENGINE=InnoDB; """
)
TABLES['player'] = (
    """ CREATE TABLE player (id INT PRIMARY KEY, name VARCHAR(50), position VARCHAR(50), dateOfBirth VARCHAR(25), countryOfBirth VARCHAR(25), nationality VARCHAR(25)) ENGINE=InnoDB; """
)
TABLES['teamXcompetition'] = (
    """ CREATE TABLE teamXcompetition (id_competition INT, id_team INT, FOREIGN KEY (id_competition) REFERENCES competition(id), FOREIGN KEY (id_team) REFERENCES team(id)) ENGINE=InnoDB; """
)
TABLES['teamXplayer'] = (
    """ CREATE TABLE teamXplayer (id_team INT, id_player INT, FOREIGN KEY (id_team) REFERENCES team(id), FOREIGN KEY (id_player) REFERENCES player(id)) ENGINE=InnoDB; """
)


#Conexión a la base de datos
conn = db_connection = mysql.connector.connect(
            user='root',
            password='',
            host='127.0.0.1',
        )
cursor = conn.cursor()

#Funcion de creacion de db 
def create_db():     
    try:
        #Se intenta fijar la db, comprobar su existencia
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Base de Datos {} inexistente.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            try:
                cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
                print("Base de datos {} creada satisfactoriamente.".format(DB_NAME))
                #Seleccionamos db
                conn.database = DB_NAME
            except mysql.connector.Error as err:
                print("Error en la creación de la Base de Datos: {}".format(err))
                exit(1)
        else:
            print(err)
            exit(1)

#Funcion de creacion de tablas    
def create_tables():
    #Crear tablas
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creando tabla {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Ya existe.")
            else:
                print(err.msg)
        else:
            print("OK")

        
#inicializar datos
def initialize():
    #Crear base de datos inicial
    create_db() 
    #Crear tablas
    create_tables()
    
#Funciones para verificar existencias de tuplas
def competitionExists(code_league):
    query = " SELECT * FROM competition WHERE code = %s"
    cursor.execute(query, (code_league, ))
    result = cursor.fetchall()
    if (result == []): 
        return True
    else:
        return False
    
def teamExists(id_team):
    query = " SELECT * FROM team WHERE (id = {} ) ".format(id_team)
    cursor.execute(query)
    result = cursor.fetchall()
    if (result == []):
        return True
    else:
        return False

def playerExists(id_player):
    query = "SELECT * FROM player WHERE (id = {} ) ".format(id_player)
    cursor.execute(query)
    result = cursor.fetchall()
    if (result == []):
        return True
    else:
        return False
    
def add_teamXcompetition(id_competition, id_team):
    #Preparacion de datos
    new_teamXcompetition = (
        "INSERT INTO teamXcompetition (id_competition, id_team) VALUES (%s, %s)"
    )
    #Insercion
    data_teamXcompetition = (id_competition, id_team)

    cursor.execute(new_teamXcompetition, data_teamXcompetition)
    conn.commit()

def add_teamXplayer(id_team, id_player):
    #Preparacion de datos
    new_teamXplayer = (
        " INSERT INTO teamXplayer (id_team, id_player) VALUES (%s, %s);"
    )
    data_teamXplayer = (id_team, id_player)
    
    #Insercion
    cursor.execute(new_teamXplayer, data_teamXplayer)
    conn.commit()
    
def import_league(code_league):
    try:
        #Generar mensaje y http status
        response = {
            'message': 'Importado satisfactoriamente',
            'status': 201 
            }
        status_code = 201
        
        #Obtener competicion desde la api
        competition = get_competition(code_league)
        
        print('Preparando importación...')
        #agregar competicion a la bd
        id_competition = competition['id']
        code_competition = competition['code']
        name_competition = competition['name']
        areaName_competition = competition['area']['name']
        
        #Preparacion de datos
        new_competition = (
            " INSERT INTO competition (id, code, name, areaName) VALUES (%s, %s, %s, %s);"
        )
        data_competition = (id_competition, code_competition, name_competition, areaName_competition)
        
        print('Cargando datos... Esto puede llevar unos minutos')
        #Insercion
        cursor.execute(new_competition, data_competition)
        conn.commit()

        #Obtener equipos de la competicion
        teams = get_teams(id_competition)

        for i in range(0, len(teams)):
            #Relacionar competicion con team
            id_team = teams[i]['id']

            #Verificar que no esté el team en la bd, de lo contrario, cargarlo
            if (teamExists(id_team)):
                #Recogemos datos
                name_team = teams[i]['name']
                tla_team = teams[i]['tla']
                shortName_team = teams[i]['shortName']
                areaName_team = teams[i]['area']['name']
                email_team = teams[i]['email']
                #Preparacion de datos
                new_team = (
                    " INSERT INTO team (id, name, tla, shortName, areaName, email) VALUES (%s, %s, %s, %s, %s, %s);"
                )
                data_team = (id_team, name_team, tla_team, shortName_team, areaName_team, email_team)

                #insercion en team
                cursor.execute(new_team, data_team)
                conn.commit()

                #Luego agregar a la tabla teamxCompetition
                add_teamXcompetition(id_competition, id_team)
            else:
                #Agregar a teamxcompetition si el registro ya existe
                add_teamXcompetition(id_competition, id_team)

            #Recorrer jugadores del team y agregar a la bd
            players = get_players(id_team)
            
            for j in range(0, len(players)):
                #Relacionar player con team
                id_player = players[j]['id']

                #Verificar que no esté el player en la bd, de lo contrario, cargarlo
                if (playerExists(id_player)):
                    #Recogemos datos
                    name_player = players[j]['name']
                    position_player = players[j]['position']
                    dateOfBirth_player = players[j]['dateOfBirth']
                    countryOfBirth_player = players[j]['countryOfBirth']
                    nationality_player = players[j]['nationality']
    
                    #Preparacion de datos
                    new_player = (
                        "INSERT INTO player (id, name, position, dateOfBirth, countryOfBirth, nationality) VALUES (%s, %s, %s, %s, %s, %s);"
                    )
                    data_player = (id_player, name_player, position_player, dateOfBirth_player, countryOfBirth_player, nationality_player)
                    
                    #Insercion en player
                    cursor.execute(new_player, data_player)
                    conn.commit()

                    #Agregar en teamxplayer
                    add_teamXplayer(id_team, id_player)
                else:
                    add_teamXplayer(id_team, id_player)

        print('Carga de datos realizada con éxito')
        return response, status_code
    except Exception as err:
        response = {
            'message': 'No encontrado.' + str(err),
            'status': 404 
            }
        status_code = 404
        return response, status_code
    
def total_players(code_league):
    #Obtener id
    query = " SELECT id FROM competition WHERE code = %s"
    cursor.execute(query, (code_league, ))
    id_league = cursor.fetchone()[0]
    #Obtener total
    query = """ SELECT SUM(players) as total 
                FROM(
                SELECT id_team as team, COUNT(id_player) as players
                FROM ( 
                SELECT id_team FROM `teamxcompetition` AS tc WHERE id_competition = '%s') AS tc 
                INNER JOIN `teamxplayer` AS tp USING(id_team) 
                GROUP BY tc.id_team) as txp """
    cursor.execute(query, (id_league, ))
    total = cursor.fetchone()[0]
    return total
                    
    
        
    


    
    