from app import create_app


app = create_app()

'''
urls = app.url_map._rules
print(urls)
print(urls[0].__dir__())
print('------')
print(urls[1].endpoint)
for url in urls:
    print("{}->{}".format(url,url.endpoint))
'''
if __name__ == "__main__":

    app.run(host=app.config['HOST'],port=app.config['PORT'],debug=app.config['DEBUG'])