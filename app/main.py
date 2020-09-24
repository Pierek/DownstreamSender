from flask import Flask
app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World from Flask in a uWSGI Nginx Docker casdasdasdasdontainer with \
     Python 3.7 (from the example template)"

@app.route("/aa")
def helloa():
    test = 'test'
    return test

    
if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True, port=81)
