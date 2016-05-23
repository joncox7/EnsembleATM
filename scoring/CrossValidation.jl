module CrossValidation

export getScore

using DataFrames

# This function partitions the datframe into n equally size dataframes
function getFolds(df::DataFrame,nFolds::Int)
    nPerFold = floor(Integer,size(df,1)/nFolds)
    ind = randperm(nPerFold*nFolds)
    folds = Dict{Int,DataFrame}()
    newDF = DataFrame()
    for name in names(df)
        newDF[name] = zeros(eltype(df[name]),nPerFold)
    end
    for i = 1:nFolds
        fold = deepcopy(newDF)
        for j = 1:nPerFold
            for name in names(fold)
                fold[name][j] = df[name][ind[(nPerFold*i-nPerFold)+j]]
            end
        end
        folds[i] = fold
    end
    return folds::Dict
end

# This function returns the cross-validated root mean squared error and Brier scores
function getScore(df::DataFrame,getPredictor::Function,nFolds=10::Int)
    folds = getFolds(df,nFolds)
    wse = zeros(Float64,6)
    newDF = DataFrame()
    for name in names(df)
        newDF[name] = eltype(df[name])[]
    end
    for i = 1:nFolds
        train = deepcopy(newDF)
        test = deepcopy(newDF)
        for j = 1:nFolds
            if j != i
                train = vcat(train,folds[j])
                #append!(train,[folds[j]])
            else
                test = vcat(test,folds[j])
                #append!(test,[folds[j]])
            end
        end
        predictor = getPredictor(train)
        for k = 1:size(test,1)
            wse += weightedSquaredError(test[k,names(test)],predictor)
        end
    end
    rwse = sqrt(wse/floor(size(df,1)))
    return rwse::Array
end

include("weightedSquaredError.jl")

end # module
