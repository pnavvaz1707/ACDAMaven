package org.example.MongoDB;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class Ejercicio3 {
    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb+srv://pnav:17072003@cluster0.8awmtte.mongodb.net/?retryWrites=true&w=majority");
        MongoDatabase db = client.getDatabase("BDPeliculas");
        MongoCollection<Document> col = db.getCollection("peliculas");

        ejercicioUno(col);

        ejercicioDos(col);

        ejercicioTres(col);

        ejercicioCuatro(col);

        ejercicioCinco(col);

        ejercicioSeis(col);

        ejercicioSiete(col);

        ejercicioOcho(col);
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

    public static void ejercicioTres(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio tres");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();
        dbObjects.add(new BasicDBObject("genres","Drama"));
        dbObjects.add(new BasicDBObject("$in",new BasicDBObject("actors.starring","Brad Pitt")));

        consulta.put("$and",dbObjects);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioCuatro(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio cuatro");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();
        dbObjects.add(new BasicDBObject("$in",new BasicDBObject("actors.starring","Morgan Freeman")));
        dbObjects.add(new BasicDBObject("$in",new BasicDBObject("actors.cast","Morgan Freeman")));

        consulta.put("$or",dbObjects);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioCinco(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio cinco");

        BasicDBObject consulta = new BasicDBObject();

//        List<BasicDBObject> dbObjects = new ArrayList<>();
        List<String> actoresLista = new ArrayList<>();
        actoresLista.add("Paulette Goddard");
        actoresLista.add("Florence Lee");
//        dbObjects.add(new BasicDBObject("$all",new BasicDBObject("actors.cast",actoresLista)));

//        consulta.put("$or",dbObjects);
        consulta.put("$all",new BasicDBObject("actors.cast",actoresLista));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioSeis(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio seis");

        BasicDBObject consulta = new BasicDBObject();


        consulta.put("size",3);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioSiete(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio siete");

        BasicDBObject consulta = new BasicDBObject();


        consulta.put("year",2010);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioOcho(MongoCollection<Document> col){
        imprimirTitulo("Ejercicio ocho");

        BasicDBObject consulta = new BasicDBObject();


        consulta.put("year",2010);

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