package org.example;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        try {
            MongoClient client = MongoClients.create("mongodb+srv://pnav:17072003@cluster0.8awmtte.mongodb.net/?retryWrites=true&w=majority");

            MongoDatabase db = client.getDatabase("BDPeliculas");
            System.out.println("Accede a bd --> " + db.getName());

            MongoCollection<Document> collection = db.getCollection("peliculas");

//            Document document1 = new Document();
//            Document document2 = new Document();
//            document2.append("calle","Virgen de las Flores");
//            document2.append("numero",23);
//            document1.append("nombre","Enrique Moyano");
//            document1.append("direccion",document2);
//            document1.append("asignaturas", Arrays.asList("ACDA","PSP","SIGE","ENDE"));
//
//            System.out.println(collection.insertOne(document1));

//            System.out.println(collection.updateOne(Filters.eq("direccion.calle", "Mármoles"), Updates.set("direccion.barrio", "Trinidad")));
//            System.out.println(collection.updateOne(Filters.eq("direccion.calle","Mármoles"), Updates.set("direccion.numero",23)));
//            System.out.println(collection.updateOne(Filters.eq("direccion.calle", "Mármoles"), Updates.set("asignaturas", Arrays.asList("ACDA", "PSP", "SIGE", "ENDE"))));

//            System.out.println(collection.deleteOne(Filters.eq("nombre", Pattern.compile("riq"))));

            Document buscarDocument = new Document("year", "1998");
            FindIterable<Document> resultadoBusqueda = collection.find(buscarDocument);
            for (Document document : resultadoBusqueda) {
                System.out.println(document.get("title") + " del año " + document.get("year"));
                Document actores = (Document) document.get("actors");
                List<String> actoresList = actores.getList("cast", String.class);
                for (String s : actoresList) {
                    System.out.println(s);
                }
            }

//            MongoCursor<Document> cursor = resultadoBusqueda.cursor();
//            while (cursor.hasNext()){
//                System.out.println(cursor.next().get("title"));
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}