library(dplyr)
#c1<-c('JO LTD', 'LX03 2PZ', 'Active', 'Mr Patrick','Mrs Li')
#c2<-c('XYZ', 'LX03 2PZ', 'Dissolved', 'Mr Patrick','ksjdhf')
#c3<-c('lkjsd', 'E11 2NA', 'Active', 'Mr Patrick','Mrs Li')
#c4<-c('kjsahd','slkajd','Dissolved','Mrs Li','fd')



Comps<-read_csv("Desktop/Data_Acc/comp_facts_data.csv")


Compdir<-read_csv("Desktop/Data_Acc/Director_data.csv")

####need to change this as it doesn't work with the current director structure###
#create an adj matrix for number of common directors
adj_dir<-matrix(0,4,4)
#loop through compdir rows and assign that as adj row
for(i in 1:4){
  #loop through compdir col
  for(n in 1:2){
    #loop through compdir row to compare to and assign that as adj col
    for(k in 1:4){
      #loop through compdir col to compare to
      for(m in 1:2){
        if(i!=k && Compdir[i,n]==Compdir[k,m])
        {adj_dir[i,k]<-adj_dir[i,k]+1}
      }
    }
  }
  
}


Comps_facts<-subset(Comps,select=c(company_number, company_name, address_line_1, address_line_2, care_of, locality, postal_code, sic_code))
#Adjacency matrix without directors
adj<-matrix(0,nrow(Comps_facts),nrow(Comps_facts))

#loop the rows in company table and assign that as adj row
for(i in 1:nrow(Comps_facts)) {
  #loop the columns in company table to compare to
  for(j in 1:ncol(Comps_facts)){
    #loop the rows in company table to compare to and assign that as adj col
    for(n in 1:nrow(Comps_facts)){
      if(i!=n &&Comps_facts[i,j]==Comps_facts[n,j]){
        adj[i,n]<-adj[i,n]+1
        }
    
    }
  }
}

final_adj<-matrix(0,4,4)
#add the adj matrices together
for(i in 1:4){
  for(j in 1:4){
    final_adj[i,j]<-adj[i,j]+adj_dir[i,j]
  }
}
