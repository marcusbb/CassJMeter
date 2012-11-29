package com.rim.icrs.cassjmeterext.gui;

import java.awt.GridBagConstraints;

import javax.swing.BorderFactory;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.border.BevelBorder;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.connections.a6x.EnhancedAstyanaxConnection;
import com.rim.icrs.cassjmeterext.gui.components.CompositeSerializerList;
import com.rim.icrs.cassjmeterext.sampler.EnhancedAbstractSampler;
import com.rim.icrs.cassjmeterext.sampler.EnhancedGetSampler;

public class EnhancedGet extends EnhancedAbstractGUI
{    
 	private static final long serialVersionUID = -2385526600604989215L;
	private static final String LABEL = "Cassandra Enhanced Get";    
	private JTextField CNAME;
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    
    private CompositeSerializerList COMPOSITE_KSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_CSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_VSERIALIZERS_LIST;
   
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        CNAME.setText(element.getPropertyAsString(EnhancedGetSampler.COLUMN_NAME));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.VALUE_SERIALIZER_TYPE));
        COMPOSITE_KSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_KEY_SERIALIZER_TYPES), ","));
        COMPOSITE_CSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_COLUMN_SERIALIZER_TYPES), ","));
        COMPOSITE_VSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_VALUE_SERIALIZER_TYPES), ","));       
    }
    
    public TestElement createTestElement()
    {
    	EnhancedGetSampler sampler = new EnhancedGetSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");     
        return sampler;
        
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof EnhancedGetSampler)
        {
        	EnhancedGetSampler gSampler = (EnhancedGetSampler) sampler;
        	gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setColumnName(CNAME.getText());            
                   
            if("CompositeSerializer".equals(gSampler.getKSerializerType())){
            	gSampler.setCompositeKSerializerTypes(COMPOSITE_KSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getCSerializerType())){
            	gSampler.setCompositeCSerializerTypes(COMPOSITE_CSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getVSerializerType())){
            	gSampler.setCompositeVSerializerTypes(COMPOSITE_VSERIALIZERS_LIST.getListModelAsArray());
            }
        }
    }

    public void initFields()
    {     	
        CNAME.setText("${__Random(1,1000)}");
        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
    	
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, CNAME = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, KSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 5, COMPOSITE_KSERIALIZERS_LIST = new CompositeSerializerList());
        KSERIALIZER.addActionListener(COMPOSITE_KSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, 6, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 6, CSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 7, COMPOSITE_CSERIALIZERS_LIST = new CompositeSerializerList());
        CSERIALIZER.addActionListener(COMPOSITE_CSERIALIZERS_LIST);
        
        JLabel infoLabel = new JLabel("Use clientType <" + EnhancedAstyanaxConnection.class.getName() + "> in CassandraProperties panel if better results display is desired for Composite Value Types", JLabel.LEFT);
        infoLabel.setBorder(BorderFactory.createBevelBorder(BevelBorder.LOWERED));
        labelConstraints.gridwidth = GridBagConstraints.REMAINDER;
    	addToPanel(mainPanel, labelConstraints, 0, 8, infoLabel);    	
    	//reset to 1
    	labelConstraints.gridwidth = 1;

        addToPanel(mainPanel, labelConstraints, 0, 9, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 9, VSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 10, COMPOSITE_VSERIALIZERS_LIST = new CompositeSerializerList());
        VSERIALIZER.addActionListener(COMPOSITE_VSERIALIZERS_LIST);
        
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }    
}
