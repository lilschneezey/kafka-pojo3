package io.schneezey.avro.reflect;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;

public class PojoSerDes<P> {
	private Schema schema = null;
	private static Logger logger = Logger.getLogger(PojoSerDes.class);
	private boolean ignoreNulls = false;
	
	public PojoSerDes() {
	}
	public PojoSerDes(boolean ignoreNulls) {
		this.ignoreNulls = ignoreNulls;
	}
	
	public P deserializePojo( GenericRecord record, P target ) throws Exception {
		if (schema == null) {
			schema = ReflectData.get().getSchema(target.getClass());			
			logger.info( schema.toString(true) );
		}
		if (record == null) return null;
		if (schema.getType() != Schema.Type.RECORD) {
			logger.error("Pojo needs to be a basic class: Schema.Type.RECORD");
		}
		logger.info("Processing Schema.Type.RECORD: " +  schema.getFullName());

		for (Schema.Field field : schema.getFields()) {
			deserializeField( field, field.schema(), record, target);
		}
		return target;
	}
	

	/*
	 * fields must be of type primitive.  e.g. int.class/Integer.Type, not Class Integer.
	 * class String supported
	 * 
	 */

	private void deserializeField(Field metaField, Schema metaFieldSchema, GenericRecord parent, Object target) throws Exception {
	
		// get the custom encoding to be used
		String customEncoding = metaFieldSchema.getProp("CustomEncoding");
		if ( customEncoding != null ) {
			logger.info( "Property: " + customEncoding );
		}
		// get the custom class definition to be used
		String javaclassEncoding = metaFieldSchema.getProp("java-class");
		if ( javaclassEncoding != null ) {
			logger.info( "Property: " + javaclassEncoding );
		}
				
		Object value = parent.get(metaField.name());
		
		// require a bean set method
		java.lang.reflect.Method objMethod;
		try {
			switch (metaFieldSchema.getType() ) {
				case BOOLEAN :
					if ( value instanceof Boolean ) {
						try {						
								objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Boolean.TYPE );
								objMethod.invoke(target, new Boolean((Boolean)value));
						} catch (NoSuchMethodException|SecurityException ex) {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Boolean.class );
							objMethod.invoke(target, new Boolean((Boolean)value));
						}
					}
					break;
				case DOUBLE :
					if ( value instanceof Double ) {
						try {												
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Double.TYPE );
							objMethod.invoke(target, new Double((Double)value));
						} catch (NoSuchMethodException|SecurityException ex) {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Double.class );
							objMethod.invoke(target, new Double((Double)value));
						}
					}
					break;			
				case FLOAT: 
					if ( value instanceof Float ) {
						try {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Float.TYPE );
							objMethod.invoke(target, new Float((Float)value));
						} catch (NoSuchMethodException|SecurityException ex) {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Float.class );
							objMethod.invoke(target, new Float((Float)value));
						}
					}
					break;	
				case INT :
					if ( value instanceof Integer ) {
						try {						
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Integer.TYPE );
							objMethod.invoke(target, new Integer((Integer)value));
						} catch (NoSuchMethodException|SecurityException ex) {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Integer.class );
							objMethod.invoke(target, new Integer((Integer)value));
						}
					}
					break;				
				case LONG :
					if ( value instanceof Long ) {
						if ( customEncoding == "DateAsLongEncoding") {
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), java.util.Date.class );
							objMethod.invoke(target, new java.util.Date((Long)value));
						} else {
							try {
								objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Long.TYPE );
								objMethod.invoke(target, new Long((Long)value));
							} catch (NoSuchMethodException|SecurityException ex) {
								objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), Long.class );
								objMethod.invoke(target, new Long((Long)value));
							}
						}
					}
					break;		
				case STRING :
					//TODO change to CharSequence
					if ( value instanceof Utf8 ) {
						objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), String.class );
						objMethod.invoke(target, new String(((Utf8)value).toString()) );
					}
					break;
				case ENUM :
					if ( value instanceof GenericEnumSymbol ) {
						@SuppressWarnings("rawtypes")
						Class enumClazz =  Class.forName(metaFieldSchema.getNamespace()+metaFieldSchema.getName());
						objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), enumClazz );
						
						GenericEnumSymbol enumvalue = (GenericEnumSymbol)value;
						@SuppressWarnings({ "rawtypes", "unchecked" })
						Enum enumValue = Enum.valueOf(enumClazz, enumvalue.toString() );
						
						objMethod.invoke(target, enumValue );
					}
					break;
				case BYTES :
					if ( value instanceof ByteBuffer ) {
						objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), byte[].class );
						byte[] bbclone = ((ByteBuffer)value).array().clone();
						objMethod.invoke(target, bbclone );
					}		
					break;
				case UNION:
					if ( value instanceof GenericRecord ) {
						GenericRecord unionvalue = (GenericRecord)value;
						for (Schema unionElementSchema : metaFieldSchema.getTypes()) {
							if ( unionElementSchema.equals(unionvalue.getSchema()) ) {
								deserializeField( metaField, unionElementSchema, parent, target);
							}
						}
					} else if ( value instanceof GenericArray ) {
						@SuppressWarnings("rawtypes")
						GenericArray unionvalue = (GenericArray)value;
						for (Schema unionElementSchema : metaFieldSchema.getTypes()) {
							if ( unionElementSchema.equals(unionvalue.getSchema()) ) {
								deserializeField( metaField, unionElementSchema, parent, target);
							}
						}
					} else {
						// This is for Nullable primatives using a full Class. aka: Integer rather than int.
						boolean isNullable = false;
						for (Schema unionElementSchema : metaFieldSchema.getTypes()) {
							switch ( unionElementSchema.getType() ) {
								case NULL:
									// Nullable fields always appear
									isNullable = true;
									break;
								case BOOLEAN:
								case DOUBLE:
								case FLOAT:
								case INT:
								case LONG:
								case BYTES:
								case STRING:
									if (isNullable) deserializeField( metaField, unionElementSchema, parent, target);
									break;
								default:
									break;
							}
						}
					}
					// TODO: proper Unions of many different types and classes.
					// The Nullable test I do above is not needed if proper Unions are handled.
					break;

					// throws forName --> ClassNotFoundException
					// throws newInstance --> InstiantiationException, IllegalAccessException, 
					// throws getMethod --> NoSuchMethod, SecurityException
				case RECORD :
					if (value instanceof GenericRecord) {
						//set up the target class to populate
						Object targetRecord;
						if ( javaclassEncoding != null )
							targetRecord = Class.forName(javaclassEncoding).newInstance();
						else
							targetRecord = Class.forName(metaFieldSchema.getFullName()).newInstance();
						
						// iterate through each of the fields and process
						for (Schema.Field field: metaFieldSchema.getFields()) {
							deserializeField( field, field.schema(), (GenericRecord)value, targetRecord );			
						}
						
						// set into the parent object
						objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), targetRecord.getClass() );
						objMethod.invoke(target, targetRecord);
					}
					break;

				case ARRAY :
					if ( value instanceof GenericArray && metaFieldSchema.getElementType().getType() == Type.RECORD ) 
					{
						if (javaclassEncoding != null) {
							Collection<?> targetCollection = (Collection<?>)Class.forName(javaclassEncoding).newInstance();
							
							@SuppressWarnings("unchecked")
							GenericArray<GenericRecord> sourceArray = (GenericArray<GenericRecord>) value;
							Schema recordSchema = metaFieldSchema.getElementType();
							
							// go through source Array and create copies
							for (Iterator<GenericRecord> iterator = (Iterator<GenericRecord>)sourceArray.iterator(); iterator.hasNext();) {
								GenericRecord sourceRecord = iterator.next();	
								deserializeRecord(recordSchema, sourceRecord, targetCollection);
							}

							// set into the parent object
							objMethod = target.getClass().getMethod("set" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1), targetCollection.getClass() );
							objMethod.invoke(target, targetCollection);
						}
					} else {
						throw new Exception("Invalid array element type.  Must be a Type.RECORD :" + metaFieldSchema.getElementType().getType().toString());
					}
					break;
				case FIXED :
				case MAP :
				case NULL :
					break;
			}
		} catch (Exception ex) {
			// no public method available with the correct mixed case name
			// or error invoking setMethod
			logger.error( ex.toString() );
			throw ex;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void deserializeRecord(Schema recordSchema, GenericRecord sourceRecord, Collection targetCollection)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, // new instance
			Exception // deserializeField
	{

		// TODO this should not be in the loop
		// get the custom class definition to be used
		String javaclassEncoding = recordSchema.getProp("java-class");
		if (javaclassEncoding != null) {
			logger.info("Property: " + javaclassEncoding);
		}

		// create a target pojo
		Object targetRecord;
		if (javaclassEncoding != null)
			targetRecord = Class.forName(javaclassEncoding).newInstance();
		else
			targetRecord = Class.forName(recordSchema.getFullName()).newInstance();

		// iterate through each of the fields and process
		for (Schema.Field field : recordSchema.getFields()) {
			deserializeField(field, field.schema(), (GenericRecord) sourceRecord, targetRecord);
		}	
		
		targetCollection.add(targetRecord);
	}

	public GenericRecord serializePojo(P pojo) throws Exception {
		if (schema == null) {
			schema = ReflectData.get().getSchema(pojo.getClass());			
			logger.info( schema.toString(true) );
		}
		if ( pojo == null) return null;
		GenericRecordBuilder builder = new GenericRecordBuilder(schema);
		if (schema.getType() != Schema.Type.RECORD) {
			logger.error("Pojo needs to be a basic class: Schema.Type.RECORD");
		}
		logger.info("Processing Schema.Type.RECORD: " +  schema.getFullName());
		
		for (Schema.Field field: schema.getFields()) {
			serializeField( field, pojo, builder );			
		}
		
		return builder.build();
	}


	private void serializeField(org.apache.avro.Schema.Field metaField, Object parent, GenericRecordBuilder parentBuilder) throws Exception {
		serializeField(metaField, metaField.schema(), parent, parentBuilder);
	}
	private void serializeField(org.apache.avro.Schema.Field metaField, Schema metaFieldSchema, Object parent, GenericRecordBuilder parentBuilder) throws Exception {
		
		logger.info("Processing Schema.Type." + metaFieldSchema.getType().getName() + " : " + metaField.name() );
		
		/*
		 *  retrieve the get Method
		 *  Boolean & boolean are special cases.  Worse, @Nullable Booleans will appear in a union structure.  
		 *  So to make it simple, we just try "get" first, then "is".    
		 */
		Object value = null;
		try {
			java.lang.reflect.Method objMethod = parent.getClass().getMethod("get" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1));
			value = objMethod.invoke(parent);
		} catch ( NoSuchMethodException|SecurityException getEx) {
			try {
				java.lang.reflect.Method objMethod = parent.getClass().getMethod("is" + metaField.name().substring(0, 1).toUpperCase() + metaField.name().substring(1));
				value = objMethod.invoke(parent);
			} catch ( NoSuchMethodException|SecurityException isEx) {
				// no public method available with the correct mixed case name
				logger.error( getEx.toString() );
				logger.error( isEx.toString() );
				throw isEx;
			}
		}
		
		if (value == null && ignoreNulls) return;
		
		// get the custom encoding to be used
		String customEncoding = metaFieldSchema.getProp("CustomEncoding");
		if ( customEncoding != null ) {
			logger.info( "Property: " + customEncoding );
		}
		// get the custom class definition to be used
		String javaclassEncoding = metaFieldSchema.getProp("java-class");
		if ( javaclassEncoding != null ) {
			logger.info( "Property: " + javaclassEncoding );
		}
		
		switch (metaFieldSchema.getType() ) {
			case BOOLEAN :
			case DOUBLE :
			case FLOAT: 
			case INT :
			case LONG :
			case STRING :
				if (customEncoding == "DateAsLongEncoding" && value == null) {
					Exception nullDateException = new Exception("Encountered null value for DateAsLong encoded field.  Avro reflection does not observe this combination. " + metaField.name());
					logger.error(nullDateException.toString());
					throw nullDateException;
				} else if (customEncoding == "DateAsLongEncoding" && value.getClass().isAssignableFrom(java.util.Date.class)) {
					value = ((java.util.Date)value).getTime();
				}
				parentBuilder.set(metaField, value );
				break;
			case ENUM :
				GenericData.EnumSymbol enumField;
				enumField = new GenericData.EnumSymbol(metaFieldSchema,value);
				parentBuilder.set(metaField,  enumField);
				break;
			case BYTES :
				byte[] bytevalue = (byte[])value;
				parentBuilder.set(metaField, java.nio.ByteBuffer.allocate(bytevalue.length).put(bytevalue) );
				break;
			case UNION :
				boolean isNullable = false;
				for (Schema unionSchema: metaFieldSchema.getTypes()) {
					
					if (unionSchema.getType() == Schema.Type.NULL ) {
						// all nullable fields turn into 2-part unions
						// Nullable always occurs first. use it as an indicator
						isNullable = true;
						if ( value == null ) {
							// if field is actually null, ignore field in mapping
							break;
						}
					} else if ( value == null ) {
						logger.error("Encountered null value for non-nullable field: " + metaField.name());
					} if ( isNullable ) {
						// don't worry about matching types on a simple nullable union.  too complicated.
						serializeField(metaField, unionSchema, parent, parentBuilder);
					} else {
						// if a complex union, we need to match types correctly
						String unionClassEncoding = unionSchema.getProp("java-class");
						if ( unionClassEncoding != null ) {
							logger.info( "Property: " + unionClassEncoding );
						}
						String unionClassName = value.getClass().getName();
						if (unionClassName == unionClassEncoding || unionClassName == unionSchema.getFullName()) {
							serializeField(metaField, unionSchema, parent, parentBuilder);
						}
					}
				}
				break;
			case RECORD :
				GenericRecordBuilder recordBuilder = new GenericRecordBuilder(metaFieldSchema);
				for (Schema.Field field: metaFieldSchema.getFields()) {
					serializeField( field, value, recordBuilder );			
				}
				parentBuilder.set(metaField, recordBuilder.build());
				break;
			case ARRAY :
				if ( metaFieldSchema.getElementType().getType() == Type.RECORD) {
					Object[] array;
					if (value instanceof Collection ) {
						array = ((Collection<?>)value).toArray();
					} else if ( value instanceof Object[] ) {
						array = (Object[])value;
					} else {
						throw new Exception("Invalid array element type :" + value.getClass().getCanonicalName());
					}
					GenericData.Array<GenericRecord> targetArray = new GenericData.Array<GenericRecord>(array.length, metaFieldSchema);
					for (int i = 0; i < array.length; i++) {
						GenericRecordBuilder elementBuilder = new GenericRecordBuilder(metaFieldSchema.getElementType());
						for (Schema.Field field : metaFieldSchema.getElementType().getFields()) {
							serializeField( field, array[i], elementBuilder);
						}
						targetArray.add(i, elementBuilder.build());
					}
					parentBuilder.set(metaField, targetArray);
				} else {
					throw new Exception("Invalid array element type.  Must be a Type.RECORD :" + metaFieldSchema.getElementType().getType().toString());
				}
				break;
			case FIXED :
			case MAP :
			case NULL :
				break;
		}
	}

}
