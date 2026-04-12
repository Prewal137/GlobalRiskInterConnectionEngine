import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import styled from 'styled-components';

const NavbarContainer = styled.nav`
  background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
  padding: 1rem 2rem;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  position: sticky;
  top: 0;
  z-index: 1000;
`;

const NavContent = styled.div`
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const Logo = styled(Link)`
  color: white;
  font-size: 1.5rem;
  font-weight: bold;
  text-decoration: none;
  display: flex;
  align-items: center;
  gap: 0.5rem;

  &:hover {
    transform: scale(1.05);
    transition: transform 0.2s;
  }
`;

const NavLinks = styled.div`
  display: flex;
  gap: 1rem;
`;

const NavLink = styled(Link)`
  color: ${(props) => (props.$active ? '#FFD700' : 'white')};
  text-decoration: none;
  padding: 0.5rem 1rem;
  border-radius: 8px;
  background: ${(props) => (props.$active ? 'rgba(255, 215, 0, 0.2)' : 'transparent')};
  font-weight: ${(props) => (props.$active ? 'bold' : 'normal')};
  transition: all 0.3s;

  &:hover {
    background: rgba(255, 255, 255, 0.1);
    transform: translateY(-2px);
  }
`;

const Navbar = () => {
  const location = useLocation();

  return (
    <NavbarContainer>
      <NavContent>
        <Logo to="/">🌍 Global Risk Platform</Logo>
        <NavLinks>
          <NavLink to="/" $active={location.pathname === '/'}>
            📊 Live Dashboard
          </NavLink>
          <NavLink to="/history" $active={location.pathname === '/history'}>
            📅 Historical
          </NavLink>
          <NavLink to="/state" $active={location.pathname === '/state'}>
            🗺️ State Analysis
          </NavLink>
          <NavLink to="/whatif" $active={location.pathname === '/whatif'}>
            ⚙️ What-If
          </NavLink>
        </NavLinks>
      </NavContent>
    </NavbarContainer>
  );
};

export default Navbar;
