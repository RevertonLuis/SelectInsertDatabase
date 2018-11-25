import numpy

class Polygon():
   
   def bordas(self):

      self.b = numpy.where( (self.pv == 0.) & (self.pe <= 0) )
      
      #if self.b[0].shape[0] > 0:
      #   print "O ponto (%s, %s) encontra-se na borda do poligono" % (self.yp, self.xp)
      #   
      #   if self.b[0].shape[0] == 1:
      #      print "e pertence ao segmento de reta %s" % (self.b[0][0]+1) 
      #   
      #   else:
      #      print "e pertence ao vertice dos segmentos de reta %s e %s" % (self.b[0][0]+1, self.b[0][1]+1)     
      #      
      #else:
      #   print "O ponto (%s, %s) nao esta na borda do poligono" % (self.yp, self.xp)
      
      #if self.b[0].shape[0] > 0:
      #   self.borda = 1
      
      #Os possiveis valores de self.b[0].shape[0] sao 0 (fora da borda), 1 (no segmento de reta), 2 (no vertice)
      #desta forma 0 = fora, 1 e 2 = na borda do poligono
      self.borda = self.bordas_case[self.b[0].shape[0]]
      
         

   def calcula_cruzamentos(self):
      
      # Determinando onde ocorrem os cruzamentos 
      self.cruzamentos = numpy.where( (self.pv < 0.) & (self.ya_c < self.yp) & (self.yp <= self.yb_c) )

      # nc = numero de cruzamentos
      #self.nc = self.cruzamentos[0].shape[0]

      # Se cruzamentos for impar entao o ponto esta dentro do poligono
      #if self.nc % 2 == 1: ou 
      #   self.dentro = 1.
         
      # O passo abaixo substitui os passos acima
      self.dentro = self.cruzamentos[0].shape[0] % 2
	 

   def calcula_pv_pe(self):

      # Calculo de produto vetorial
      self.pv = (self.xb - self.xa)*(self.yp - self.ya) - (self.xp - self.xa)*(self.yb-self.ya)

      # Mudando a direcao de um vetor o produto vetorial tambem muda de direcao
      self.pv = numpy.where( self.yb < self.ya, -1*self.pv, self.pv)
      
      # Calculo de pe (produto escalar) de PA*PB
      #Obs: Produto escalar nao tem direcao entao nao precisa inverter o sinal
      #alem disso e PA*PB, se mudarmos A com B fica PB*PA e sabe-se que PA*PB = PB*PA
      self.pe = (self.xa-self.xp)*(self.xb - self.xp) + (self.ya-self.yp)*(self.yb-self.yp)
            
   
   def decide(self, yp, xp):
       
      # Inicializando as variaveis do ponto    
      self.xp = xp
      self.yp = yp
      self.dentro = 0 
         
      #Calculando os termos pv (produto vetorial) e pe (produto escalar)
      self.calcula_pv_pe()
      
      # Verificando se o ponto na borda do poligono
      self.bordas() 
      
      if self.borda == 1: # ou seja, esta na borda do poligono
         self.dentro = self.bordas_dentro
      
      else: # ou seja, nao esta na borda do poligono (ou dentro ou fora)
        
	 # Verificando o numero de cruzamentos
         self.calcula_cruzamentos()
      
   def __init__(self, y, x, bordas_dentro=1):
                                    
      # "Declaracao" das variavies
      if isinstance(y, list) and isinstance(x, list):
         y = numpy.array(y, 'f')
         x = numpy.array(x, 'f')

      self.xa = numpy.zeros(x[:-1].shape, 'f')
      self.xb = numpy.zeros(x[:-1].shape, 'f')
      self.ya = numpy.zeros(y[:-1].shape, 'f')
      self.yb = numpy.zeros(y[:-1].shape, 'f')
      
      # Definicao dos termos xa e xb
      self.xa[:] = x[:-1]
      self.xb[:] = x[1:]
   
      # Definicao dos termos ya e yb
      self.ya[:] = y[:-1]
      self.yb[:] = y[1:]
      
      # Se ya > yb entao trocar ya com yb (tentar remover este passo)
      self.ya_c = numpy.where( self.ya > self.yb, self.yb, self.ya)
      self.yb_c = numpy.where( self.ya > self.yb, self.ya, self.yb)
      
      # Se bordas_dentro = 0 entao pontos nas bordas nao sao considerados como dentro do poligono
      # Se bordas_dentro = 1 entao pontos nas bordas sao considerados como dentro do poligono
      self.bordas_dentro = bordas_dentro
      self.bordas_case = [0,1,1]	 
      
      
