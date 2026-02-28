

# player_blob = bucket.blob(player_path_info)
# player_json = player_blob.download_as_text()

# player_df = pd.read_json(io.StringIO(player_json))
# project_id = "fpl-analytics"
# table_id = f"raw_player_data.players-{date.today().isoformat()}"

# pandas_gbq.to_gbq(player_df, table_id, project_id=project_id)