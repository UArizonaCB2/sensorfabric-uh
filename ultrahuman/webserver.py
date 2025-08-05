#!/usr/bin/env python3

"""
Used only for local testing to render the reporting templates.
Do not include this in the production lambda build.
"""

from flask import Flask, request
from templates import lambda_handler

app = Flask(__name__)

@app.route('/')
def index():
    event = {
        'participant_id': 'BB-3234-3734',
    }
    html = lambda_handler(event, None)
    return html['body']

if __name__ == '__main__':
    app.run(debug=True)
