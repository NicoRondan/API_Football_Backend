import http.client
import json

#API KEY de prueba
API_KEY = 	'6b8f9f49b2d8449b8ec6c5eb4ba6f037'

#Conexion con la api
connection = http.client.HTTPConnection('api.football-data.org')
headers = { 'X-Auth-Token': API_KEY }

#Obtener competicion en base a su id
def get_competition(code): 
    connection.request('GET', '/v2/competitions/{}'.format(code), None, headers )
    response = json.loads(connection.getresponse().read().decode())
    return response

#Obtener equipos en base al id de la competicion
def get_teams(id_competition): 
    connection.request('GET', '/v2/competitions/{}/teams'.format(id_competition), None, headers )
    response = json.loads(connection.getresponse().read().decode())
    result = response['teams']
    return result

#Obtener jugadores en base al id del equipo
def get_players(id_team): 
    connection.request('GET', '/v2/teams/{}'.format(id_team), None, headers )
    response = json.loads(connection.getresponse().read().decode())
    result = response['squad'] 
    return result

