from flask import Flask, Response, jsonify, request
from db import initialize, competitionExists, import_league, total_players

app = Flask(__name__)
   
#Inicializar datos
initialize()

@app.route('/import-league/<code_league>')
def importLeague(code_league):
    try:
        code_league = code_league.upper()
        #Comprobar si el codigo de liga ya se importó anteriormente
        if (competitionExists(code_league)):
            response, status_code = import_league(code_league)
            response = jsonify(response)
            response.status_code = status_code
            return response
        else:
            response = jsonify({
            'message': 'Competicion ya importada',
            'status': 409 
            })
            response.status_code = 409
            return response
    except Exception as err:
        response = jsonify({
            'message': 'Error del servidor: ' + str(err),
            'status': 504 
            })
        response.status_code = 504
        return response

@app.route('/total-players/<code_league>')
def totalPlayers(code_league):
    code_league = code_league.upper()
    #Comprobar si el codigo de liga existe en la base de datos
    if (competitionExists(code_league)):
        response = jsonify({
        'message': 'Código de competición no encontrado.',
        'status': 404
        })
        response.status_code = 404
        return response
    else:
        #Calcular total de jugadores en la liga
        total = total_players(code_league)
        response = jsonify({
        'message': 'Total: ' + str(total),
        'status': 200
        })
        response.status_code = 200
        return response

#En caso de error de ruta
@app.errorhandler(404)
def not_found(error=None):
    response = jsonify({
        'message': 'Página no encontrada: ' + request.url,
        'status': 404 
    })
    response.status_code = 404
    return response

if __name__ == '__main__':
    app.run(port = 3000, debug = False)

