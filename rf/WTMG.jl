module WTMG

export getPredictorWTMG

using DataFrames, PyCall

function append_to_python_search_path(str::AbstractString)
    unshift!(PyVector(pyimport("sys")["path"]), str)
end

function getWTMGParams(nFeatures)
    return RFfitparams(50,"mse",floor(Integer,nFeatures/3),5,true,0)::RFfitparams
end

function predictionModel(df::DataFrame)
    input = convert(Array,[df[:AAR_0] df[:Wind_1] df[:Wind_Dir_1] df[:Wind_Gust_1] df[:Ceiling_1] df[:Visibility_1]])
    params = getWTMGParams(size(input,2))
    labels = convert(Array,df[:AAR_1])
    return fit_rf(input,labels,params)::PyObject
end

function samplingModel(pm::PyObject,df::DataFrame)
    maxAAR = maximum(df[:AAR_0])
    sm = zeros(Float64,6,maxAAR+1,maxAAR+1)
    predictions = zeros(size(df,1),6)
    for i = 1:size(sm,1)
        predictions[:,i] = round(Integer,predictLabel(pm,convert(Array,[df[:AAR_0] df[symbol("Wind_"*string(i))] df[symbol("Wind_Dir_"*string(i))] df[symbol("Wind_Gust_"*string(i))] df[symbol("Ceiling_"*string(i))] df[symbol("Visibility_"*string(i))]])))
    end
    for i = 1:size(predictions,1)
        for j = 1:size(predictions,2)
            sm[j,predictions[i,j]+1,df[symbol("AAR_"*string(j))][i]+1] += 1.
        end
    end
    normalize(sm)
    return sm::Array
end

function getPredictorWTMG(df::DataFrame)
    pm = predictionModel(df)
    sm = samplingModel(pm,df)
    function predictor(df::DataFrame)
        predictions = zeros(Float64,6)
        for i = 1:length(predictions)
            predictions[i] = round(predictLabel(pm,convert(Array,[df[:AAR_0] df[symbol("Wind_"*string(i))] df[symbol("Wind_Dir_"*string(i))] df[symbol("Wind_Gust_"*string(i))] df[symbol("Ceiling_"*string(i))] df[symbol("Visibility_"*string(i))]]))[1])
        end
        probs = zeros(Float64,6,size(sm,2))
        for i = 1:size(probs,1)
            probs[i,:] = sm[i,predictions[i]+1,:]
        end
        return probs::Array
    end
    return predictor::Function
end

include("normalize.jl")
include("randomForests.jl")
append_to_python_search_path("./rf")
@pyimport rfr 

end # module
