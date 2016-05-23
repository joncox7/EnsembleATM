function normalize(mm::Array)
    dim = length(size(mm))
    if dim == 2
        for i = 1:size(mm,1)
            if sum(mm[i,:]) != 0.
                mm[i,:] /= sum(mm[i,:])
            end
        end
    elseif dim == 3
        for i = 1:size(mm,1)
            for j = 1:size(mm,2)
                if sum(mm[i,j,:]) != 0.
                    mm[i,j,:] /= sum(mm[i,j,:])
                end
            end
        end
    elseif dim == 4
        for i = 1:size(mm,1)
            for j = 1:size(mm,2)
                for k = 1:size(mm,3)
                    if sum(mm[i,j,k,:]) != 0.
                        mm[i,j,k,:] /= sum(mm[i,j,k,:])
                    end
                end
            end
        end
    end
    return mm
end
