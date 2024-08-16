from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
import pickle

app = Flask(__name__)
CORS(app,resources={r"/*": {"origins": "*", "allow_headers":{"Access-Control-Allow-Origin"}}})
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/api/model', methods=['POST'])
@cross_origin(origin='*',headers=['content-type'])
def model():
    if request.method == 'POST':
        f = request.files.get('file')
        df = pd.read_csv(f)

        X = df.drop('Species', axis=1)
        y = df['Species']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        knn = KNeighborsClassifier(n_neighbors=5)
        knn.fit(X_train, y_train)

        # save the model with pickle
        filename = 'knn.sav'
        pickle.dump(knn, open(filename, 'wb'))

        #load the model with pickle
        loaded_model = pickle.load(open(filename, 'rb'))
        
        y_pred = loaded_model.predict(X_test)
        acc = loaded_model.score(X_test, y_test)

        return jsonify({'model' : filename})
    

if __name__ == '__main__':
    app.run(debug=True)
       
 
        