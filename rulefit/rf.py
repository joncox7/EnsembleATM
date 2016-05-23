import numpy as np
import pandas as pd

from rulefit import RuleFit

def fitrf(train, labels):
    r = RuleFit()
    r.fit(train, labels)
    return r

def predict(r, test):
    return r.predict(test)
