package es.udc.fi.ri;

public class WriteIndex {

    public static void main(String[] args){

        String indexPath = "index";
        String outputpath = null;
        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-outputfile":
                    outputpath = args[++i];
                    break;
            }
        }
    }
}
