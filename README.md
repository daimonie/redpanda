# redpanda
redpanda is a collection of pandas pipes that are for general use


## pipe_where
Because of the speed of `np.where`, many people choose to use it. I've seen a number of constructions where there is a group of conditions to assign one category, and otherwise we assign the negative category. However, nesting `np.where` under PEP8 leads to a horrible code. The notebook introduces a pipe-able function that takes care of the nested where construction, while allowing the code to look readable.

## pipe_check_many_to_many
2 calls to check if a join has led to an increase in rows and if that is wanted
