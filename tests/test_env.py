


key_columns = ["plan_code", "effective_from",]

merge_condition = " AND ".join([f"t.{c} = s.{c}" for c in key_columns])

print(merge_condition)