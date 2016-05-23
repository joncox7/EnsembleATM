function fit_rf(train::Array, labels::Array)
    pred = rf.fitrf(train, labels)
    return pred::PyObject
end

function predictLabel(r::PyObject, test::Array)
    return rf.predict(r, test)::Array
end
