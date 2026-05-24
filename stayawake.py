# so you can host the selfbot 24/7
from flask import Flask
from threading import Thread

mainpoint = Flask('')

@mainpoint.route('/')
def home():
    return "started host"

def run():
    mainpoint.run(host='0.0.0.0', port=4338)

def host():
    t = Thread(target=run)
    t.start()
