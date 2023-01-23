package org.example.MongoDB;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;

public class Ejercicio3 {
    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb+srv://pnav:17072003@cluster0.8awmtte.mongodb.net/?retryWrites=true&w=majority");
        MongoDatabase db = client.getDatabase("BDPeliculas");
        MongoCollection<Document> col = db.getCollection("peliculas");

        ejercicioUno(col);

        ejercicioDos(col);
    }

    private static void imprimirTitulo(String titulo){
        for (int i = 0; i < titulo.length(); i++) {
            System.out.print("-");
        }
        System.out.println();
        System.out.println(titulo);
        for (int i = 0; i < titulo.length(); i++) {
            System.out.print("-");
        }
        System.out.println();
    }

    public static void ejercicioUno(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio uno");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("storyline", new BasicDBObject("$regex","crime").append("$options","i"));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioDos(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio dos");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("genres", "Drama");

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }
}