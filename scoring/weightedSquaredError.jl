function weightedSquaredError(test::DataFrame,predictor::Function)
    wse = zeros(Float64,6)
    n = size(test,1)
    for i = 1:n
        prediction = predictor(test[i,names(test)])
        for j = 1:length(wse)
            for k = 1:size(prediction,2)
                wse[j] += prediction[j,k]*(k-1-test[symbol("AAR_"*string(j))][1])^2
            end
        end
    end
    return wse::Array
end
