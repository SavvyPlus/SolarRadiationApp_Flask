from flask import Flask
from flask import render_template

app = Flask(__name__)


@app.route('/test')
def hello_world():
    return 'Hello World!'


@app.route('/')
def index():
    posts = [{'a': 'aaaa'}, {'a': 'bb'}]

    return render_template('index.html', title='awsom', posts=posts)


if __name__ == '__main__':
    app.run()
