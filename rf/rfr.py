import numpy as np
from sklearn.ensemble import RandomForestRegressor

def fitrf(train, labels, n_estimators, criterion, max_features, min_samples_leaf, bootstrap, random_state):
    r = RandomForestRegressor(n_estimators=n_estimators, criterion=criterion, max_features=max_features, min_samples_leaf=min_samples_leaf, bootstrap=bootstrap, random_state=random_state)
    r.fit(train, labels)
    return r

def predict(r, test):
    return r.predict(test)
