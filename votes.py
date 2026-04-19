#https://api.oireachtas.ie/v1/votes?chamber_type=house&chamber_id=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&chamber=dail&date_start=2026-04-12&skip=0&limit=50&outcome=Carried

#adding  scafalding for votes API endpoint, which will be used to extract voting data for TDs and Senators. This will include information on how each member voted on different bills and motions, as well as the overall outcome of each vote. This data will be critical for analyzing voting patterns and understanding the legislative behavior of members of the Oireachtas. The code will also include functions for saving the extracted data in a structured format (e.g., JSON or CSV) for further analysis and visualization in the utility app.
#working url 

#key fields, debate, debateSection, pdf (optional)
#chamber
#members['member']['showAs'] - name of member
#members['member']['memberCode'] - name of member
#Ta votes
#taVotes['taVotes]['showAs'] - vote yes  

#tallies 
#nilVotes['nilVotes]['showAs'] - vote no  
#['outcome'] - overall outcome of vote, e.g. carried, lost, etc.
#divsion['isBill] = True/False - whether the vote is on a bill or not
#tally
#debateSection	"dbsect_18"

#voteId	"vote_80"

#Staon (abstain?)