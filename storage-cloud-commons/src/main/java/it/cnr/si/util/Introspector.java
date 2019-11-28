/*
 * Copyright (C) 2019  Consiglio Nazionale delle Ricerche
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as
 *     published by the Free Software Foundation, either version 3 of the
 *     License, or (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 *
 *     You should have received a copy of the GNU Affero General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package it.cnr.si.util;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class Introspector implements Serializable {
    private static Hashtable properties = new Hashtable();


    public static Object getPropertyValue(Object obj, String s)
            throws IntrospectionException, InvocationTargetException {
        if (s == null)
            return obj;
        try {
            for (StringTokenizer stringtokenizer = new StringTokenizer(s, "."); stringtokenizer.hasMoreElements(); ) {
                String s1 = stringtokenizer.nextToken();
                if (obj == null)
                    return null;
                PropertyDescriptor propertydescriptor = getProperyDescriptor(obj.getClass(), s1);
                if (propertydescriptor == null)
                    try {
                        Field field = obj.getClass().getField(s);
                        obj = field.get(obj);
                    } catch (NoSuchFieldException _ex) {
                        if (obj instanceof SelfIntrospector)
                            return ((SelfIntrospector) obj).getPropertyValue(s);
                        else
                            throw new IntrospectionException(obj.getClass() + " non possiede la propriet\340 " + s1);
                    }
                else
                    obj = propertydescriptor.getReadMethod().invoke(obj, null);
            }

        } catch (IllegalAccessException illegalaccessexception) {
            throw new IllegalAccessError(illegalaccessexception.getMessage());
        }
        return obj;
    }

    private static PropertyDescriptor getProperyDescriptor(Class class1, String s)
            throws IntrospectionException {
        return (PropertyDescriptor) getProperyDescriptors(class1).get(s);
    }

    private static Hashtable getProperyDescriptors(Class class1)
            throws IntrospectionException {
        Hashtable hashtable = (Hashtable) properties.get(class1);
        if (hashtable == null)
            synchronized (properties) {
                hashtable = (Hashtable) properties.get(class1);
                if (hashtable == null) {
                    PropertyDescriptor apropertydescriptor[] = java.beans.Introspector.getBeanInfo(class1).getPropertyDescriptors();
                    properties.put(class1, hashtable = new Hashtable());
                    for (int i = 0; i < apropertydescriptor.length; i++)
                        try {
                            hashtable.put(apropertydescriptor[i].getName(), new ListPropertyDescriptor(apropertydescriptor[i], class1));
                        } catch (IntrospectionException _ex) {
                            hashtable.put(apropertydescriptor[i].getName(), apropertydescriptor[i]);
                        }

                    properties.put(class1, hashtable);
                }
            }
        return hashtable;
    }

    public static Object invoke(Object obj, Method method, Object... aobj)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if (method == null)
            throw new NoSuchMethodException("No method in class " + obj.getClass() + " with parameters specified.");
        else
            return method.invoke(obj, aobj);
    }
}
