type RFfitparams
    n_estimators        :: Int              # number of trees in the forest
    criterion           :: AbstractString   # function to measure quality of split 
    max_features        :: Int              # number of features to consider when looking for the best split
    min_samples_leaf    :: Int              # minimum number of samples in newly created leaves
    bootstrap           :: Bool             # whether bootstrap samples are used when constructing trees
    random_state        :: Int              # seed
end

function fit_rf(train::Array, labels::Array, params::RFfitparams)
    pred = rfr.fitrf(train, labels, params.n_estimators, params.criterion, params.max_features, params.min_samples_leaf, params.bootstrap, params.random_state)
    return pred::PyObject
end

function predictLabel(r::PyObject, test::Array)
    return rfr.predict(r, test)::Array
end
