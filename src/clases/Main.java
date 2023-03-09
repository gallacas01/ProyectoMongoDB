package clases;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import org.bson.Document;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Accumulators.min;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;

public class Main {

	static ConnectionString connectionString = new ConnectionString("mongodb://localhost:27017");
	static MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
	static MongoClient mongoClient = MongoClients.create(settings);
	static MongoDatabase db = mongoClient.getDatabase("proyectoMongo");
	static MongoCollection<Document> coleccionProductos = db.getCollection("productos");
	static MongoCollection<Document> coleccionProveedores = db.getCollection("proveedores");

	private static void insertArticulos() {
		Document prod1 = new Document();
		prod1.put("_id", 1);
		prod1.put("nombre", "Macbook Pro");
		prod1.put("precio", 2000.00);
		prod1.put("categorias", Arrays.asList("ordenador", "portátil"));
		prod1.put("stock", true);
		prod1.put("especificaciones", new Document("cpu", "Inter Core i-7").append("ram", 16).append("memoria", 1024));
		prod1.put("devolucion", null);

		Document prod2 = new Document();
		prod2.put("_id", 2);
		prod2.put("nombre", "HP 15S-eq2088ns");
		prod2.put("precio", 840.00);
		prod2.put("categorias", Arrays.asList("ordenador", "portátil"));
		prod2.put("stock", false);
		prod2.put("especificaciones",
				new Document("cpu", "AMD Ryzen 7 5700U").append("ram", 16).append("memoria", 1024));
		prod2.put("devolucion", true);

		Document prod3 = new Document();
		prod3.put("_id", 3);
		prod3.put("nombre", "Lenovo IdeaPad 3 15ITL6");
		prod3.put("precio", 649.99);
		prod3.put("categorias", Arrays.asList("ordenador", "portátil"));
		prod3.put("stock", true);
		prod3.put("especificaciones",
				new Document("cpu", "Intel Core i5-1135G7").append("ram", 16).append("memoria", 512));
		prod3.put("devolucion", null);

		Document prod4 = new Document();
		prod4.put("_id", 4);
		prod4.put("nombre", "Zone Evil Gold79");
		prod4.put("precio", 2074.21);
		prod4.put("categorias", Arrays.asList("ordenador", "sobremesa"));
		prod4.put("stock", false);
		prod4.put("especificaciones",
				new Document("cpu", "Intel Core i7-11700F").append("ram", 32).append("memoria", 2048));
		prod4.put("devolucion", true);

		Document prod5 = new Document();
		prod5.put("_id", 5);
		prod5.put("nombre", "Acer Aspire XC-1760");
		prod5.put("precio", 499.00);
		prod5.put("categorias", Arrays.asList("ordenador", "sobremesa"));
		prod5.put("stock", true);
		prod5.put("especificaciones",
				new Document("cpu", "Intel Core i5-12400F").append("ram", 16).append("memoria", 1024));
		prod5.put("devolucion", null);

		try {
			coleccionProductos.insertOne(prod1);
			coleccionProductos.insertOne(prod2);
			coleccionProductos.insertOne(prod3);
			coleccionProductos.insertOne(prod4);
			coleccionProductos.insertOne(prod5);
			System.out.println("Inserción de registros en la colección de productos realizada satisfactoriamente.");
		} catch (MongoWriteException e) {
			System.out.println("Los productos ya han sido insertados en la base de datos.");
		}

	}

	private static void insertProveedor() {

		Document proveedor1 = new Document();
		proveedor1.put("_id", 1);
		proveedor1.put("nombre", "TecnoOnuba");
		proveedor1.put("direccion", new Document().append("poblacion", "Huelva").append("calle", "Pablo Rada, 8")
				.append("codPostal", 21001));
		proveedor1.put("vehiculos", Arrays.asList("coche", "furgoneta"));
		proveedor1.put("telefono", 956816193);
		proveedor1.put("productos", Arrays.asList(2, 3, 4, 5));
		proveedor1.put("nacional", true);

		Document proveedor2 = new Document();
		proveedor2.put("_id", 2);
		proveedor2.put("nombre", "CPU Galicia");
		proveedor2.put("direccion", new Document().append("poblacion", "Finisterre")
				.append("calle", "Calle de la Coruña, 13").append("codPostal", 15155));
		proveedor2.put("vehiculos", Arrays.asList("coche", "furgoneta", "barco"));
		proveedor2.put("telefono", 956816193);
		proveedor2.put("productos", Arrays.asList(1, 3, 5));
		proveedor2.put("nacional", true);

		Document proveedor3 = new Document();
		proveedor3.put("_id", 3);
		proveedor3.put("nombre", "TaviraGraphics");
		proveedor3.put("direccion", new Document().append("poblacion", "Tavira").append("calle", "Rua da liberdade")
				.append("codPostal", 8800));
		proveedor3.put("vehiculos", Arrays.asList("furgoneta"));
		proveedor3.put("telefono", 900065839);
		proveedor3.put("productos", Arrays.asList(1, 2, 4, 5));
		proveedor3.put("nacional", false);

		try {
			coleccionProveedores.insertOne(proveedor1);
			coleccionProveedores.insertOne(proveedor2);
			coleccionProveedores.insertOne(proveedor3);
			System.out.println("Inserción de registros en la colección de proveedoreds realizada satisfactoriamente.");
		} catch (MongoWriteException e) {
			System.out.println("Los proveedores ya han sido insertados en la base de datos.");
		}

	}
	
	private static void mostrarProductos() {
		MongoCursor<Document> cursorProductos = coleccionProductos.find().iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void mostrarColeccionProveedores() {
		Iterator itColeccionProveedores = coleccionProveedores.find().iterator();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("***********Colección de proveedores**********");
		while (itColeccionProveedores.hasNext()) {
			System.out.println(itColeccionProveedores.next());
		}
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");

	}
	
	private static void mostrarColeccionProductos() {
		Iterator itColeccionProductos = coleccionProductos.find().iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("***********Colección de proveedores**********");
		while (itColeccionProductos.hasNext()) {
			System.out.println(itColeccionProductos.next());
		}
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta1
	private static void consulta1() {
		MongoCursor<Document> cursorProductos8o16Ram = coleccionProductos
				.find(or(eq("especificaciones.ram", 8), eq("especificaciones.ram", 16))).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos cuya memoria ram es de 8 o 16 GB**********");
		while (cursorProductos8o16Ram.hasNext()) {
			Document docProducto = cursorProductos8o16Ram.next();
			System.out.println(docProducto.toJson());
		}
		cursorProductos8o16Ram.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Constulta2
	private static void consulta2() {
		MongoCursor<Document> cursorProductos = coleccionProductos.find().sort(Sorts.descending("precio")).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos ordenado descendentemente por precio**********");
		while (cursorProductos.hasNext()) {
			Document docProducto = cursorProductos.next();
			System.out.println(docProducto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta3
	private static void consulta3() {
		Document productoMasBarato = coleccionProductos.find().sort(Sorts.ascending("precio")).first();
		System.out.println(	"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Producto más barato**********");
		System.out.println(productoMasBarato.toJson());
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");

	}

	// Consulta4
	private static void consulta4() {
		long numOrdenadoresSobremesa = coleccionProductos
				.count(eq("categorias", Arrays.asList("ordenador", "sobremesa")));
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("Número de ordenadores  de sobremesa: " + numOrdenadoresSobremesa);
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta5
	private static void consulta5() {
		MongoCursor<Document> cursorProveedoresNacionales = coleccionProveedores.find(eq("nacional", true)).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Proveedores nacionales**********");
		while (cursorProveedoresNacionales.hasNext()) {
			Document docProveedor = cursorProveedoresNacionales.next();
			System.out.println(docProveedor.toJson());
		}
		cursorProveedoresNacionales.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta6
	private static void consulta6(double precio) {
		MongoCursor<Document> cursorProductos = coleccionProductos.find(gte("precio", precio)).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos que cuestan más de " + precio + "€**********");
		while (cursorProductos.hasNext()) {
			Document docProveedor = cursorProductos.next();
			System.out.println(docProveedor.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta7
	private static void consulta7(int almacenamiento) {
		MongoCursor<Document> cursorProductos = coleccionProductos.find(ne("especificaciones.memoria", almacenamiento)).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out
				.println("**********Productos que cuya memoria ROM es distinta de " + almacenamiento + " GB**********");
		while (cursorProductos.hasNext()) {
			Document docProveedor = cursorProductos.next();
			System.out.println(docProveedor.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta8	
	private static void consulta8() {
		Document producto = coleccionProductos.find(eq("_id", 2)).first();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Producto antes de actualizar el campo stock***********");
		System.out.println(producto.toJson());
		coleccionProductos.updateOne(eq("_id", 2), set("stock", true));
		System.out.println("**********Producto tras actualizar el campo stock***********");
		Document productoActualizado = coleccionProductos.find(eq("_id", 2)).first();
		System.out.println(productoActualizado.toJson());
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta9
	private static void consulta9() {
		coleccionProductos.updateMany(gte("precio", 2000.00), set("dto", 15));
		MongoCursor<Document> cursorProductos = coleccionProductos.find(gte("precio", 2000.00)).iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos a los que se ha añadido un campo 'dto'**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");

	}

	// Consulta10
	private static void consulta10() {
		coleccionProductos.updateMany(exists("dto"), unset("dto"));
		MongoCursor<Document> cursorProductos = coleccionProductos.find(or(eq("_id", 4), eq("_id", 1))).iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos cuyo campo 'dto' ha sido eliminado**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta11
	private static void consulta11() {
		coleccionProveedores.updateOne(eq("_id", 1), set("numTrabajadores", 200));
		coleccionProveedores.updateOne(eq("_id", 2), set("numTrabajadores", 134));
		coleccionProveedores.updateOne(eq("_id", 3), set("numTrabajadores", 176));
		MongoCursor<Document> cursorProveedores = coleccionProveedores.find().iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Proveedores**********");
		while (cursorProveedores.hasNext()) {
			Document docProveedor = cursorProveedores.next();
			System.out.println(docProveedor.toJson());
		}
		cursorProveedores.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta12
	private static void consulta12() {
		coleccionProveedores.updateOne(eq("nacional", false), inc("numTrabajadores", -26));
		Document proveedorExtranjero = coleccionProveedores.find(eq("nacional", false)).first();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Proveedor cuyo número de trabajadores se ha visto reducido en 26**********");
		System.out.println(proveedorExtranjero.toJson());
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta13
	private static void consulta13() {
		coleccionProveedores.updateMany(in("_id", Arrays.asList(1, 2, 3)),
				rename("numTrabajadores", "numeroDeTrabajadores"));
		MongoCursor<Document> cursorProveedores = coleccionProveedores.find().iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Proveedores**********");
		while (cursorProveedores.hasNext()) {
			Document proveedor = cursorProveedores.next();
			System.out.println(proveedor.toJson());
		}
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");

	}

	// Consulta14
	private static void consulta14() {
		coleccionProductos.updateMany(in("_id", Arrays.asList(1, 2, 3, 4, 5)),
				pushEach("categorias", (Arrays.asList("tecnología", "novedad"))));
		MongoCursor<Document> cursorProductos = coleccionProductos.find().iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos cuyo campo 'categorias' ha sido actualizado**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta15
	private static void consulta15() {
		coleccionProductos.updateMany(in("_id", Arrays.asList(1, 2, 3, 4, 5)), pull("categorias", "novedad"));
		MongoCursor<Document> cursorProductos = coleccionProductos.find().iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Ordenadores cuyo campo elemento 'novedad' del campo 'categorias' ha sido eliminado**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	// Consulta16
	private static void consulta16() {
		MongoCursor<Document> cursorProductos = coleccionProductos
				.find(and(eq("especificaciones.ram", 16), lt("especificaciones.memoria", 1024))).iterator();
		System.out.println(
				"\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos que tienen una memoria RAM de 16 GB y su memoria ROM es menor a 1024GB**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}
	
	// Consulta17
		private static void consulta17() {
			MongoCursor<Document> cursorProveedores = coleccionProveedores
					.find(in("direccion.codPostal", Arrays.asList(21001, 8800))).iterator();
			System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
			System.out
					.println("**********Proveedores cuyo codPostal se encuentra entre los valores [21001,8800]**********");
			while (cursorProveedores.hasNext()) {
				Document producto = cursorProveedores.next();
				System.out.println(producto.toJson());
			}
			cursorProveedores.close();
			System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		}

	// Consulta18
	private static void consulta18() {
		coleccionProductos.updateOne(eq("_id", 1), set("especificaciones.tarjetaDeRed", true));
		coleccionProductos.updateOne(eq("_id", 2), set("especificaciones.tarjetaDeRed", false));
		coleccionProductos.updateOne(eq("_id", 3), set("especificaciones.tarjetaDeRed", true));
		MongoCursor<Document> cursorProductos = coleccionProductos.find(or(eq("_id", 1), eq("_id", 2), eq("_id", 3)))
				.iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Productos a los que se le ha añadido un campo 'tarjetaDeRed' dentro del documento 'especificaciones'**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void consulta19() {
		Document cursorProductos = coleccionProductos.aggregate(Arrays.asList(group("",	min("precioMinimo", "$precio"),
				max("precioMaximo", "$precio"),avg("precioMedio", "$precio"), sum("precioTotal", "$precio")))).first();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Datos sobre el precio de los ordenadores**********");
		System.out.println(cursorProductos.toJson());
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void consulta20() {
		MongoCursor<Document> cursorProductos = coleccionProductos
				.aggregate(Arrays.asList(group("$especificaciones.memoria", avg("precioMedio", "$precio")),
						sort(Sorts.ascending("_id")))).iterator(); //El _id de dentro del sort hace referencia al _id de la función group
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Precio medio de los ordenadores según su almacenamiento**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void consulta21() {
		MongoCursor<Document> cursorProductos = coleccionProductos
				.aggregate(Arrays.asList(match(eq("stock", true)), project(fields(include("_id", "especificaciones"))))).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Procesadores de aquellos productos que tengan stock**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void consulta22() {
		coleccionProductos.updateOne(eq("_id", 1), set("unidadesRestantes", 100));
		coleccionProductos.updateOne(eq("_id", 2), set("unidadesRestantes", 64));
		coleccionProductos.updateOne(eq("_id", 3), set("unidadesRestantes", 50));
		coleccionProductos.updateOne(eq("_id", 4), set("unidadesRestantes", 0));
		coleccionProductos.updateOne(eq("_id", 5), set("unidadesRestantes", 90));
		Document cursorProductos = coleccionProductos.aggregate(Arrays.asList(match(lt("unidadesRestantes", 100)),
				group("", sum("sumaUnidades", "$unidadesRestantes")), project(fields(excludeId())))).first();

		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println(cursorProductos.toJson());
	}

	private static void consulta23() {
		MongoCursor<Document> cursorProductos = coleccionProductos.aggregate(Arrays.asList(
				group("$especificaciones.memoria", max("precioMaximo", "$precio"), avg("precioMedio", "$precio")),
				project(fields(include("especificaciones.memoria", "precioMaximo", "precioMedio"))),
				sort(Sorts.descending("_id")), out("datosConsulta23"))).iterator();
		System.out.println("\n///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		System.out.println("**********Precio medio de los ordenadores según su memoria ram**********");
		while (cursorProductos.hasNext()) {
			Document producto = cursorProductos.next();
			System.out.println(producto.toJson());
		}
		cursorProductos.close();
		System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");

	}

	private static void consulta24(int id) {
		DeleteResult del = coleccionProductos.deleteOne(eq("_id", id));
		if (del.getDeletedCount() > 0) {
			System.out.println("\nEl producto con id = " + id + " ha sido eliminado correctamente.");
		} else {
			System.out.println("No se ha podido eliminar el producto.");
		}
	}

	private static void consulta25() {
		DeleteResult del = coleccionProductos.deleteMany(gte("precio", 2000.0));
		if (del.getDeletedCount() > 0) {
			System.out.println("Se han eliminado aquellos productos cuyo precio era superior a 2000€.");
		}
	}

	private static void consulta26(String poblacion) {
		DeleteResult del = coleccionProveedores.deleteMany(eq("direccion.poblacion", poblacion));
		if (del.getDeletedCount() > 0) {
			System.out.println("El proveedor de '" + poblacion + "' ha sido eliminado.");
		} else {
			System.out.println("No existe ningún proveedor con esa población.");
		}
	}

	private static void consulta27(int id) {
		  Document res  =  coleccionProveedores.find(eq("_id", id)).first();
		  List <Integer> listaId = res.get("productos", List.class);
		   MongoCursor<Document> cursorProductos = coleccionProductos.find(in("_id", listaId)).iterator();
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		   System.out.println("**********PRODUCTOS DEL PROVEDOOR CON ID " + id + "***********");
		   while (cursorProductos.hasNext()) {
			   Document producto  = cursorProductos.next();
			   System.out.println(producto.toJson());
		   }
		   cursorProductos.close();
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		   
	}
	
	private static void consulta28(){
		  Document res  =  coleccionProveedores.find(eq("nacional", false)).first();
		  List <Integer> listaId = res.get("productos", List.class);
		   Document precioMedio = coleccionProductos.aggregate(Arrays.asList(				   
				   match(in("_id", listaId)),
				   group("",avg("precioMedio" ,"$precio")),
				   project(fields(include("precioMedio"), excludeId())))).first();
		   DecimalFormat df = new DecimalFormat("#.##");
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		   System.out.println("-PRECIO MEDIO DE LOS PRODUCTOS DEL PROVEEDOR EXTRANJERO : " + df.format(precioMedio.get("precioMedio")) + "€.");
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}
	
	private static void consulta29() {
		  Document res  =  coleccionProveedores.find(eq("_id", 3)).first();
		  List <Integer> listaId = res.get("productos", List.class);
		  MongoCursor<Document> cursorProductos = coleccionProductos.find(and(in("_id", listaId), eq("devolucion",null))).iterator();
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
		   System.out.println("**********PRODUCTOS DEL PRVOEEDOR 3 QUE NUNCA PODRÁN SER DEVUELTOS ");
		   while (cursorProductos.hasNext()) {
			   Document producto  = cursorProductos.next();
			   System.out.println(producto.toJson());
		   }
		   cursorProductos.close();
		   System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
	}

	private static void eliminarColeccionProductos() {
		coleccionProductos.drop();
	}
	
	private static void eliminarColeccionProveedores() {
		coleccionProveedores.drop();
	}
	
	private static void menuOpcionesPrincipales() {
		System.out.println("\n-------------------------------------OPCIONES PRINCIPALES-------------------------------------");
		System.out.println("Opción 0: INSERCIÓN AUTOMÁTICA DE DATOS");
		System.out.println("Opción 1: CONSULTAS BÁSICAS");
		System.out.println("Opción 2: CONSULTAS DE ACTUALIZACIÓN");
		System.out.println("Opción 3: CONSULTAS Y MODIFICACIONES SOBRE ARRAYS");
		System.out.println("Opción 4: CONSULTAS DE AGREGACIÓN PIPELINE");
		System.out.println("Opción 5: BORRADO DE DOCUMENTOS");
		System.out.println("Opción 6: CONSULTAS UTILIZANDO FUNCIONES PARA ARRAYS");
		System.out.println("Opción 7: CONSULTAS SOBRE DOCUMENTOS ENLAZADOS");
		System.out.println("Opción 8: MOSTRAR COLECCIONES");
		System.out.println("Opción 9: ELIMINAR COLECCIONES");
		System.out.println("Opción 10: SALILR");

	}
	
	private static void consultasBasicas() {
		System.out.println(
				"\n-----------------------------------------------CONSULTAS BÁSICAS-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Seleccionar todos los productos cuya memoria ram sea de 8 o 16 GB.");
		System.out.println("-Opción 2: Obtener todos los productos ordenados descendentemente por precio.");
		System.out.println("-Opción 3: Obtener el producto más barato.");
		System.out.println("-Opción 4: Obtener el número de ordenadores que son de sobremesa.");
		System.out.println("-Opción 5: Obtener todos los proveedores nacionales.");
		System.out.println("-Opción 6: Obtener aquellos productos cuyo precio es igual o superior al que se pasa por parámetro.");
		System.out.println("-Opción 7: Obtener aquellos productos cuyo almacenamiento(memoria ROM) sea distinta de aquella pasada "
						+ "como parámetro.");
	}

	private static void consultasDeActualizacion() {
		System.out.println("\n-----------------------------------------------CONSULTAS DE ACTUALIZACIÓN-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Modificar el stock a true del producto cuyo id=2.");
		System.out.println("-Opción 2: Insertar un campo ‘dto’ en los ordenadores con precio igual o superior a 2000€.");
		System.out.println("-Opción 3: Eliminar el campo ‘dto’ de aquellos ordenadores que lo tengan.");
		System.out
				.println("-Opción 4: Insertar el campo ‘numTrabajadores’ en cada registro de la coleccion proveedores.");
		System.out.println("-Opción 5: Decrementar en 26 el número de trabajadores del proveedor que no es nacional");
		System.out.println("-Opción 6: Modifcar el nombre del campo ‘numTrabajadores’ a ‘numeroDeTrabajadores’ de la colección de "
						+ "trabajadores.");
	}

	private static void consultasSobreArrays() {
		System.out.println("\n-----------------------------------------------CONSULTAS Y MODIFICACIONES SOBRE ARRAYS-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Listar los productos cuya memoria RAM sea de 16 GB y su memoria ROM sea menor de 1024 GB.");
		System.out.println("-Opción 2: Obtener aquellos proveedores cuyo código postal sea 21001 o 8800.");
		System.out.println("-Opción 3: Insertar en los ordenadores portátiles el campo tarjetaDeRed.");
	}
	
	private static void consultasAgregacionPipeline() {
		System.out.println("\n-----------------------------------------------CONSULTAS AGREGACIÓN PIPELINE-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Obtener el precio mínimo, precio máximo, precio medio y precio total de los ordenadores.");
		System.out.println("-Opción 2: Obtener el precio medio de los ordenadores agrupando por la memoria ROM mostrando los resultados"
				+ " ordenados por la memoria ROM descendientemente.");
		System.out.println("-Opción 3: Obtener las especificaciones de todos los productos que tengan stock.");
		System.out.println("-Opción 4: Mostar la suma de todas las unidades restantes de aquellos productos cuyas unidades restantes"
				+ " esté por debajo de 100 excluyendo el id al mostrar el resultado.");
		System.out.println("-Opción 5: Mostar el precio máximo y medio de los ordenadores filtrando por memoria RAM ordenando "
				+ "los resultados descendentemente y guardando la salida en la colección ‘datosConsulta23’");
	}
	
	private static void borradoDeDocumentos() {
		System.out.println("\n-----------------------------------------------BORRADO DE DOCUMENTOS-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Eliminar un producto cuyo id sea igual al que se pasa como parámetro.");
		System.out.println("-Opción 2: Eliminar aquellos productos cuyo precio sea superior o igual a 2000€.");
		System.out.println("-Opción 3: Eliminar el proveedor cuyo campo ‘población’ sea igual al pasado como parámetro.");
	}
	
	private static void consultasFuncionesArrays() {
		System.out.println("\n-----------------------------------------------CONSULTAS USANDO FUNCIONES PARA ARRAYS-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Insertar los valores ‘tecnología’ y ‘novedad’ dentro del array de categorías de los productos.");
		System.out.println("-Opción 2: Eliminar el elemento ‘novedad’ del array de categorías de los productos.");
	}
	
	private static void consultasSobreDocumentosEnlazados() {
		System.out.println("\n-----------------------------------------------CONSULTAS SOBRE DOCUMENTOS ENLAZADOS-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Obtener todos los productos de aquel proveedor cuyo id se pase como parámetro.");
		System.out.println("-Opción 2: Obtener el precio medio de los productos del proveedor que no es nacional.");
		System.out.println("-Opción 3: Mostrar los productos que nunca podrán ser devueltos  del proveedor 3. (devolucion = null)");
	}
	
	private static void opcionesMostrarCoelecciones() {
		System.out.println("\n-----------------------------------------------MOSTRAR COLECCIONES-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Mostrar la colección de los proveedores.");
		System.out.println("-Opción 2: Mostrar la colección de los productos.");
	}
	
	private static void opcionesEliminarColeciones() {
		System.out.println("\n-----------------------------------------------ELIMINAR COLECCIONES-----------------------------------------------");
		System.out.println("-Opción 0: Salir.");
		System.out.println("-Opción 1: Eliminar la colección de productos.");
		System.out.println("-Opción 2: Eliminar la colección de proveedores.");
	}

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		int opcion = -1;
		do {

			menuOpcionesPrincipales();
			System.out.print("Introduce una opción: ");
			opcion = sc.nextInt();
			switch (opcion) {

			case 0:
				insertArticulos();
				insertProveedor();				
				break;

			case 1:
				int opcionCaso1 = -1;
				do {
					consultasBasicas();
					System.out.print("Opción escogida: ");
					opcionCaso1 = sc.nextInt();

					if (opcionCaso1 == 1) {
						consulta1();
					} else if (opcionCaso1 == 2) {
						consulta2();
					} else if (opcionCaso1 == 3) {
						consulta3();
					} else if (opcionCaso1 == 4) {
						consulta4();
					} else if (opcionCaso1 == 5) {
						consulta5();
					} else if (opcionCaso1 == 6) {

						double precio = 0.0;
						System.out.print("Precio (escribir los decimales con coma,no con punto.): ");
						precio = sc.nextDouble();
						consulta6(precio);

					} else if (opcionCaso1 == 7) {
						int memoriaRom = 0;
						System.out.print("Introduce la memoria ROM: ");
						memoriaRom = sc.nextInt();
						consulta7(memoriaRom);
					}

				} while (opcionCaso1 != 0);

				break;

			case 2:
				int opcionCaso2 = -1;
				do {
					consultasDeActualizacion();
					System.out.print("Opción escogida: ");
					opcionCaso2 = sc.nextInt();

					if (opcionCaso2 == 1) {
						consulta8();
					} else if (opcionCaso2 == 2) {
						consulta9();
					} else if (opcionCaso2 == 3) {
						consulta10();
					} else if (opcionCaso2 == 4) {
						consulta11();
					} else if (opcionCaso2 == 5) {
						consulta12();
					} else if (opcionCaso2 == 6) {
						consulta13();
					}

				} while (opcionCaso2 != 0);
				break;

			case 3:
				int opcionCaso3 = -1;
				do {
					consultasSobreArrays();
					System.out.print("Opción escogida: ");
					opcionCaso3 = sc.nextInt();

					if (opcionCaso3 == 1) {
						consulta16();
					} else if (opcionCaso3 == 2) {
						consulta17();
					} else if (opcionCaso3 == 3) {
						consulta18();
					} 
				} while (opcionCaso3 != 0);
				break;

			case 4:
				int opcionCaso4 = -1;
				do {
					consultasAgregacionPipeline();
					System.out.print("Opción escogida: ");
					opcionCaso4 = sc.nextInt();

					if (opcionCaso4 == 1) {
						consulta19();
					} else if (opcionCaso4 == 2) {
						consulta20();
					} else if (opcionCaso4 == 3) {
						consulta21();
					} else if (opcionCaso4 == 4) {
						consulta22();
					} else if (opcionCaso4 == 5) {
						consulta23();
					}

				} while (opcionCaso4 != 0);
				break;

			case 5:
				int opcionCaso5 = -1;
				do {
					borradoDeDocumentos();
					System.out.print("Opción escogida: ");
					opcionCaso5 = sc.nextInt();

					if (opcionCaso5 == 1) {
						
						System.out.print("Introduce el id del ordenador que deseas borrar: " );
						int idOrdenador = sc.nextInt();
						consulta24(idOrdenador);
						
					} else if (opcionCaso5 == 2) {
						consulta25();
					} else if (opcionCaso5 == 3) {
						
						System.out.print("Introduce la provincia a la que pertenece el proveedor que deseas eliminar: ");
						String provincia = sc.next();
						consulta26(provincia);						
					}

				} while (opcionCaso5 != 0);
				break;

			case 6:
				int opcionCaso6 = -1;
				do {
					consultasFuncionesArrays();
					System.out.print("Opción escogida: ");
					opcionCaso6 = sc.nextInt();

					if (opcionCaso6 == 1) {						
						consulta14();
						
					}else if (opcionCaso6 == 2) {
						consulta15();
					} 

				} while (opcionCaso6 != 0);
				System.out.println("Opción1");
				break;

			case 7:
				int opcionCaso7 = -1;
				do {
					consultasSobreDocumentosEnlazados();
					System.out.print("Opción escogida: ");
					opcionCaso7 = sc.nextInt();

					if (opcionCaso7 == 1) {		
						
						System.out.print("Introduce el id del proveedor del que deseas ver sus productos: ");
						int idProveedor = sc.nextInt();
						consulta27(idProveedor);
						
					}else if (opcionCaso7 == 2) {
						consulta28();
					} 
					else if (opcionCaso7 == 3) {
						consulta29();
					} 

				} while (opcionCaso7 != 0);
				break;

			case 8:
				
				int opcionCaso8 = -1;
				do {
					opcionesMostrarCoelecciones();
					System.out.print("Opción escogida: ");
					opcionCaso8 = sc.nextInt();

					if (opcionCaso8 == 1) {							
						mostrarColeccionProveedores();						
					}else if (opcionCaso8 == 2) {
						mostrarColeccionProductos();
					}					

				} while (opcionCaso8 != 0);
				break;
			
			case 9:
				int opcionCaso9 = -1;
				do {
					opcionesEliminarColeciones();
					System.out.print("Colección a borrar: ");
					opcionCaso9 = sc.nextInt();

					if (opcionCaso9 == 1) {		
						
						eliminarColeccionProductos();
						System.out.println("\nLa colección de productos ha sido eliminada.");
						
					}else if (opcionCaso9 == 2) {
						eliminarColeccionProveedores();
						System.out.println("\nLa colección de proveedores ha sido eliminada.");
					} 

				} while (opcionCaso9 != 0);				
				break;
				
			}//Fin del switch.				

		} while (opcion <= 9);

		System.out.println("Fin del programa.");
		
		
		sc.close();
	}// Fin del main.

}// Fin de la clase.
