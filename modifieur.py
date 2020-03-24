class Modifieur:
    """
    Classe permettant de modifier les données
    avant de les insérer dans la base
    """
    

    @staticmethod
    def spliter(data):
        """
        Split les données
        """
        if data == None:
            return None
        elif data != None:
            return data.split(",")
        
    @staticmethod
    def integer(data):
        """
        Passe les chiffres de str -> int et si \\N -> None
      
        """
        if data != "\\N":
            data = int(data)
            return data
        elif data == "\\N":
            return None
    
    @staticmethod
    def reindex(data):
        """
        Change l'index généré par tconst
        """
          
    