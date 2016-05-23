using DataFrames, JLD

include("tools/getData.jl")
include("scoring/CrossValidation.jl")
include("rf/WTMG.jl")
include("rulefit/RF.jl")

using CrossValidation, WTMG, RF

data = getData()

sfo = data["sfo"]
ewr = data["ewr"]

# The random forest model
wtmg_sfo_score = getScore(sfo,getPredictorWTMG)
wtmg_ewr_score = getScore(ewr,getPredictorWTMG)

# The rulefit model
rulefit_sfo_score = getScore(sfo,getPredictorRF)
rulefit_ewr_score = getScore(ewr,getPredictorRF)
