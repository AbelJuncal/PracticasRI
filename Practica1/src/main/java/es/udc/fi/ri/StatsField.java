package es.udc.fi.ri;

public class StatsField {


    public static void main(String[] args){
        boolean optionField = false;

        for(int i = 0; i < args.length; i++){
            switch (args[i]){
                case "-index":
                    break;
                case "-field":
                    optionField = true;
                    break;
            }
        }
    }

}
