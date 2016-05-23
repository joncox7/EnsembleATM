using SQLite, DataFrames, HDF5, JLD

# This function collapses the dataframe into a smaller data frame containing only the variables useful for AAR prediction
function makeDataFrame(rs::ResultSet)
    n = floor(Integer,ceil(Integer,(size(rs,1)-27)/4)/24)*24
    df = DataFrame(Year=zeros(Int64,n),Month=zeros(Int64,n),Day=zeros(Int64,n),Hour=zeros(Int64,n),AAR_0=zeros(Int64,n),AAR_1=zeros(Int64,n),AAR_2=zeros(Int64,n),AAR_3=zeros(Int64,n),AAR_4=zeros(Int64,n),AAR_5=zeros(Int64,n),AAR_6=zeros(Int64,n),Wind_0=zeros(Float64,n),Wind_1=zeros(Float64,n),Wind_2=zeros(Float64,n),Wind_3=zeros(Float64,n),Wind_4=zeros(Float64,n),Wind_5=zeros(Float64,n),Wind_6=zeros(Float64,n),Wind_Dir_0=zeros(Float64,n),Wind_Dir_1=zeros(Float64,n),Wind_Dir_2=zeros(Float64,n),Wind_Dir_3=zeros(Float64,n),Wind_Dir_4=zeros(Float64,n),Wind_Dir_5=zeros(Float64,n),Wind_Dir_6=zeros(Float64,n),Wind_Gust_0=zeros(Float64,n),Wind_Gust_1=zeros(Float64,n),Wind_Gust_2=zeros(Float64,n),Wind_Gust_3=zeros(Float64,n),Wind_Gust_4=zeros(Float64,n),Wind_Gust_5=zeros(Float64,n),Wind_Gust_6=zeros(Float64,n),Ceiling_0=zeros(Float64,n),Ceiling_1=zeros(Float64,n),Ceiling_2=zeros(Float64,n),Ceiling_3=zeros(Float64,n),Ceiling_4=zeros(Float64,n),Ceiling_5=zeros(Float64,n),Ceiling_6=zeros(Float64,n),Visibility_0=zeros(Float64,n),Visibility_1=zeros(Float64,n),Visibility_2=zeros(Float64,n),Visibility_3=zeros(Float64,n),Visibility_4=zeros(Float64,n),Visibility_5=zeros(Float64,n),Visibility_6=zeros(Float64,n))

    hour = 1
    for i = 1:n*4
        if rem(i,4) == 1
            ind = 1+trunc(Integer,i/4)
            df[:Year][ind] = parse(Int,rs[1][i][1:4])
            df[:Month][ind] = parse(Int,rs[1][i][6:7])
            df[:Day][ind] = parse(Int,rs[1][i][9:10])
            df[:Hour][ind] = hour
            for j = 0:6
                df[symbol("AAR_"*string(j))][ind] = sum(rs[2][i+4*j:i+4*j+3])
                df[symbol("Wind_"*string(j))][ind] = rs[3+j][i]
                if rs[10+j][i] == NULL
                    df[symbol("Wind_Dir_"*string(j))][ind] = NA 
                else
                    df[symbol("Wind_Dir_"*string(j))][ind] = rs[10+j][i]
                end
                if rs[17+j][i] == NULL
                    df[symbol("Wind_Gust_"*string(j))][ind] = NA
                else
                    df[symbol("Wind_Gust_"*string(j))][ind] = rs[17+j][i]
                end
                if rs[24+j][i] == NULL
                    df[symbol("Ceiling_"*string(j))][ind] = NA 
                else
                    df[symbol("Ceiling_"*string(j))][ind] = rs[24+j][i]
                end 
            end
            hour += 1
            if hour > 24
                hour = 1
            end
        end
    end
    return df::DataFrame
end

# The process function sets the following missing forecast data, so that the data set contains no Inf or Nan values
# maximum ceiling height set to highest observed value in data set
# wind direction set to the most frequent wind direction in the data set
# wind gust set to 0
# remove July 2013 from data set because this month has missing forecasts
function process(df::DataFrame)
    nonInfCeils = Float64[]
    nonInfDirs = Float64[]
    for i = 1:size(df,1)
        for j = 0:6
            if typeof(df[symbol("Ceiling_"*string(j))][i]) != NAtype
                push!(nonInfCeils,df[symbol("Ceiling_"*string(j))][i])
            end
            if typeof(df[symbol("Wind_Dir_"*string(j))][i]) != NAtype
                push!(nonInfDirs,df[symbol("Wind_Dir_"*string(j))][i])
            end
        end
    end
    defaultCeil = maximum(nonInfCeils)
    defaultDir = mode(nonInfDirs)
    for i = 1:size(df,1)
        for j = 0:6
            if typeof(df[symbol("Ceiling_"*string(j))][i]) == NAtype
                df[symbol("Ceiling_"*string(j))][i] = 250.
            end
            if typeof(df[symbol("Wind_Dir_"*string(j))][i]) == NAtype
                df[symbol("Wind_Dir_"*string(j))][i] = defaultDir
            end
            if typeof(df[symbol("Wind_Gust_"*string(j))][i]) == NAtype
                df[symbol("Wind_Gust_"*string(j))][i] = 0.
            end
        end
    end
    
    deleterows!(df,collect(intersect(Set(find(df[:Year].==2013)),Set(find(df[:Month].==7))))) # remove July 2013
end

dbs = SQLiteDB[]

for i = 1:3
    push!(dbs,SQLiteDB("../sql/data_Summer_201"*string(i)*".sqlite"))
end

str = "SELECT Time,AAR,Wind_Speed_0,Wind_Speed_4,Wind_Speed_8,Wind_Speed_12,Wind_Speed_16,Wind_Speed_20,Wind_Speed_24,Wind_Dir_0,Wind_Dir_4,Wind_Dir_8,Wind_Dir_12,Wind_Dir_16,Wind_Dir_20,Wind_Dir_24,Wind_Gust_Speed_0,Wind_Gust_Speed_4,Wind_Gust_Speed_8,Wind_Gust_Speed_12,Wind_Gust_Speed_16,Wind_Gust_Speed_20,Wind_Gust_Speed_24,Ceiling_0,Ceiling_4,Ceiling_8,Ceiling_12,Ceiling_16,Ceiling_20,Ceiling_24,Visibility_Distance_0,Visibility_Distance_4,Visibility_Distance_8,Visibility_Distance_12,Visibility_Distance_16,Visibility_Distance_20,Visibility_Distance_24 FROM unified_"

sfo = vcat(makeDataFrame(query(dbs[1],str*"SFO_20110501_20110930")),makeDataFrame(query(dbs[2],str*"SFO_20120501_20121001")),makeDataFrame(query(dbs[3],str*"SFO_20130501_20131001")))

ewr = vcat(makeDataFrame(query(dbs[1],str*"EWR_20110501_20110930")),makeDataFrame(query(dbs[2],str*"EWR_20120501_20121001")),makeDataFrame(query(dbs[3],str*"EWR_20130501_20131001")))


process(sfo)
process(ewr)

for i = 1:3
    close(dbs[i])
end

# Write to dataframe
fid = jldopen("../data/airport_data.jld","w")
write(fid,"sfo",sfo[:,names(sfo)]) 
write(fid,"ewr",ewr[:,names(ewr)]) 
close(fid)
