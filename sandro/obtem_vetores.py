from cassandra.cluster import Cluster
cluster = Cluster(['172.17.0.2'])
session = cluster.connect()
session.set_keyspace('tfbda')
query1='SELECT cod_mun, sum(qtd_obt) from obt_inf_ma where cod_mun>209999 group by cod_mun allow filtering'
# Consulta o numero de obitos totais para cada município da base
qtdobtmun = session.execute(query1)
nlinha = 1; qtdmun = 0; qtdact = 0
fp1 = open('analitico.txt', 'w')
fp2 = open('resumo.txt', 'w')
for linha in qtdobtmun:
   codmun = linha[0]
   qtdobt = linha[1]
   # para os municípios que tiverm um número maior que o limite, consulta a
   # quantidade de obitos a cada ano do periodo
   if qtdobt > 100:
      query2 = 'SELECT ano, qtd_obt from obt_inf_ma where cod_mun = ' + str(codmun) + ' and ano < 2013 allow filtering' 
      qtdobtano = session.execute(query2)
      anoant = 0; vlrant = 0; qtdant = 0; qtdok = 0; qtdtotal = 0 ; qtdanos = 1
      #para cada um dos anos que consta da base, apura o investimento
      for linha2 in qtdobtano:
         ano=linha2[0]
         qtdobt=linha2[1]
         viesinvest = 0; viesobt = 0
         query3 = 'SELECT  sum(vlr_bnf) from  bsa_fam where cod_mun = ' + str(codmun) + ' and ano = ' + str(ano) +'  allow filtering'
         vlrbnf = session.execute(query3)
         for linha3 in vlrbnf:
            vlr = linha3[0]
            if vlrant > 0 :
            	# apura o viés de investimento e de mortalidade
            	# no caso do numero de óbitos, a apuração é feita de forma invertida
               	viesinvest = vlr/vlrant
               	viesobt = qtdant/qtdobt
#                print (" %2d ==> %d, %d, %d, %8.2f, %1.2f, %1.2f " % (nlinha, codmun, ano, qtdobt, vlr, viesinvest, viesobt))
                if ( (viesinvest >= 1) & (viesobt >= 1)) or ( (viesinvest <= 1) & (viesobt <= 1)):
                	qtdok = qtdok +1
                qtdtotal = qtdtotal + 1
            vlrant = (vlrant*(qtdanos -1) + vlr)/qtdanos
            qtdant = (qtdant*(qtdanos -1)+ qtdobt)/qtdanos
            qtdanos = qtdanos + 1
            if ( viesobt > 0):
            	#fp1.write(" Cod_mun= "+str(codmun)+" Ano= "+str(ano))
            	#fp1.write(" Vies_inv= "+str('{:3.3f}'.format(viesinvest))+" Vies_obt= "+str('{:3.3f}'.format(viesobt))+"\n")
            	fp1.write(str('{:3.3f}'.format(viesinvest))+" ; "+str('{:3.3f}'.format(viesobt))+"\n")
            #fp1.write(" Vlr_inv= "+str('{:9.2f}'.format(vlr))+ " Qtd_obt= "+str(qtdobt))
            
      if qtdtotal > 0:
         if (qtdok/qtdtotal >= 0.5):
         	qtdact = qtdact + 1
         qtdmun = qtdmun + 1
         print(" %d  Municipio: %d ==> Porcentagem de acerto = %2.2f - n:%d ac:%2.2f" % (nlinha, codmun, (qtdok/qtdtotal), qtdmun, qtdact))
         fp2.write(" Cod_mun= "+ str(codmun) + "  Pct_act= " +str('{:3.3f}'.format(qtdok/qtdtotal))+ "\n")
      nlinha = nlinha + 1
fp1.close()
fp2.close()
